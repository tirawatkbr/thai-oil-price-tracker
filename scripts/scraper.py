"""
Thai Oil Price Scraper — Final Architecture
============================================

แหล่งข้อมูล (เรียงตาม priority):

  ┌──────────┬──────────────────────────────────────────────────────┬──────────────┐
  │ แบรนด์   │ Source                                               │ ประเภท      │
  ├──────────┼──────────────────────────────────────────────────────┼──────────────┤
  │ PTT      │ orapiweb.pttor.com/oilservice/OilPrice.asmx          │ SOAP XML ✅  │
  │ BCP      │ oil-price.bangchak.co.th/ApiOilPrice2/en             │ JSON REST ✅ │
  │ Shell    │ shell.co.th/.../app-fuel-prices.html                 │ HTML scrape  │
  │ Caltex   │ caltex.com/th/.../fuel-prices.html                   │ HTML scrape  │
  │ Others   │ gasprice.kapook.com/gasprice.php (ข้อมูลจาก EPPO)   │ HTML scrape  │
  │ Fallback │ api.chnwt.dev/thai-oil-api/latest                    │ JSON REST    │
  └──────────┴──────────────────────────────────────────────────────┴──────────────┘

หมายเหตุ:
  - EPPO ไม่มี public REST API สำหรับ retail price by brand
  - Kapook คือ mirror HTML ของข้อมูล EPPO
  - PTT/BCP/Shell/Caltex ดึงจาก official source โดยตรง → แม่นยำกว่า Kapook
  - Esso: ถูกตัดออกตามที่กำหนด

Output: prices.json, price_history.json, Google Sheets
"""

import os, json, re, logging
from datetime import datetime
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup

try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

# ── Config ────────────────────────────────────────────────────────────────────
TZ         = ZoneInfo("Asia/Bangkok")
TODAY      = datetime.now(TZ).strftime("%Y-%m-%d")
TIMESTAMP  = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "Thai Oil Prices")
PRICES_JSON = "prices.json"
HISTORY_JSON = "price_history.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "th-TH,th;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# OIL FAMILY RULES
# จัดกลุ่มน้ำมันให้แสดงติดกัน — เรียงจาก specific → general เสมอ
# ══════════════════════════════════════════════════════════════════════════════
OIL_FAMILY_RULES: list[tuple[str, str, int]] = [
    # (regex_pattern, family_id, sort_order)
    # เบนซิน (ไม่ผสมเอทานอล)
    (r"เบนซิน",                                      "benzene95",       0),
    # G95 — premium brand names ก่อน แล้วค่อยเป็น standard
    (r"ซูเปอร์พาวเวอร์|super.?power",               "g95_super",      10),
    (r"วี.?เพาเวอร์.*95|v.?power.*95",              "g95_vpower",     11),
    (r"95.*พรีเมียม|premium.*95|hi.*premium.*97",    "g95_premium",    12),
    (r"แก๊สโซฮอล์ 95|gasohol.?95",                 "g95",            14),
    # G91
    (r"แก๊สโซฮอล์ 91|gasohol.?91",                 "g91",            20),
    # E20, E85
    (r"e20",                                          "e20",            30),
    (r"e85",                                          "e85",            40),
    # NGV
    (r"ngv",                                          "ngv",            50),
    # Diesel — premium ก่อน แล้ว B7 แล้ว B20 แล้ว regular
    (r"วี.?เพาเวอร์.*ดีเซล|v.?power.*diesel",       "diesel_vpower",  60),
    (r"ฟิวเซฟ|fuelsave",                             "diesel_fuelsave",61),
    (r"ดีเซลพรีเมียม|ดีเซลพรีเมี่ยม|hi.?premium.?diesel","diesel_premium",62),
    (r"ดีเซล b7|diesel.?b7|hi.?diesel\b",           "diesel_b7",      63),
    (r"ดีเซลหมุนเร็ว b20|b20",                       "diesel_b20",     64),
    (r"ดีเซล\b",                                      "diesel",         65),
]

def get_family(name: str) -> tuple[str, int]:
    """คืน (family_id, sort_order) จากชื่อน้ำมัน"""
    n = name.lower().strip()
    for pattern, fid, order in OIL_FAMILY_RULES:
        if re.search(pattern, n, re.IGNORECASE):
            return fid, order
    return "other", 999

def slugify(name: str) -> str:
    """แปลงชื่อน้ำมันเป็น snake_case key"""
    k = name.lower().strip()
    k = re.sub(r"[^ก-๙a-z0-9]+", "_", k)
    return k.strip("_")


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE A: PTT Official SOAP API
# URL  : orapiweb.pttor.com/oilservice/OilPrice.asmx
# Auth : ไม่ต้องใช้ — public endpoint
# Data : XML → PTTOR_DS/FUEL[]/PRODUCT, PRICE
# ══════════════════════════════════════════════════════════════════════════════
_PTT_SOAP_URL = "https://orapiweb.pttor.com/oilservice/OilPrice.asmx"
_PTT_SOAP_BODY = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CurrentOilPrice xmlns="http://www.pttor.com/" />
  </soap:Body>
</soap:Envelope>"""

def fetch_ptt() -> dict:
    log.info("  [PTT] Official SOAP API...")
    try:
        r = requests.post(
            _PTT_SOAP_URL,
            data=_PTT_SOAP_BODY.encode("utf-8"),
            headers={
                **HEADERS,
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": "http://www.pttor.com/CurrentOilPrice",
            },
            timeout=20,
        )
        r.raise_for_status()

        root = ET.fromstring(r.text)

        # ดึง FUEL elements — อาจอยู่ใน inner XML string
        fuels = root.findall(".//FUEL")
        if not fuels:
            for el in root.iter():
                if el.text and "<FUEL>" in el.text:
                    try:
                        inner = ET.fromstring(el.text)
                        fuels = inner.findall(".//FUEL")
                        break
                    except ET.ParseError:
                        pass

        oils: dict = {}
        for fuel in fuels:
            name = (fuel.findtext("PRODUCT") or "").strip()
            price_str = (fuel.findtext("PRICE") or "").strip()
            if not name or not price_str:
                continue
            try:
                price = float(price_str.replace(",", ""))
            except ValueError:
                continue
            if price <= 0:
                continue
            family, order = get_family(name)
            key = slugify(name)
            oils[key] = {"name": name, "price": price, "family": family, "order": order}
            log.info(f"    ✓ PTT | {name:38} {price:6.2f} [{family}]")

        log.info(f"  {'✅' if oils else '⚠️ '} PTT SOAP: {len(oils)} รายการ")
        return oils

    except Exception as e:
        log.warning(f"  ⚠️  PTT SOAP ล้มเหลว: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE B: BCP Official JSON API
# URL  : oil-price.bangchak.co.th/ApiOilPrice2/en
# Auth : ไม่ต้องใช้ — public endpoint ที่ BCP เปิดให้ embed
# Data : JSON → OilList[]{OilName, PriceToday, PriceTomorrow}
# พิเศษ: มีราคาพรุ่งนี้ด้วย!
# ══════════════════════════════════════════════════════════════════════════════
def fetch_bcp() -> dict:
    log.info("  [BCP] Official JSON API...")
    try:
        r = requests.get(
            "https://oil-price.bangchak.co.th/ApiOilPrice2/en",
            headers=HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        item = data[0] if isinstance(data, list) else data

        raw_list = item.get("OilList", "[]")
        oil_list: list = json.loads(raw_list) if isinstance(raw_list, str) else raw_list

        oils: dict = {}
        for oil in oil_list:
            name = oil.get("OilName", "").strip()
            try:
                price_today = float(oil.get("PriceToday", 0))
                price_tmr   = float(oil.get("PriceTomorrow", price_today))
            except (ValueError, TypeError):
                continue
            if not name or price_today <= 0:
                continue
            family, order = get_family(name)
            key = slugify(name)
            oils[key] = {
                "name": name,
                "price": price_today,
                "price_tomorrow": price_tmr,
                "family": family,
                "order": order,
            }
            diff = f" → พรุ่งนี้ {price_tmr:.2f}" if price_tmr != price_today else ""
            log.info(f"    ✓ BCP | {name:38} {price_today:6.2f}{diff} [{family}]")

        log.info(f"  {'✅' if oils else '⚠️ '} BCP API: {len(oils)} รายการ")
        return oils

    except Exception as e:
        log.warning(f"  ⚠️  BCP API ล้มเหลว: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE C: Shell Official — Playwright (Shadow DOM)
# URL    : shell.co.th/th_th/customer/fuels-and-lubricants/fuels/fuel-price.html
# ราคาอยู่ใน Shadow DOM ของ custom element <standalone-table>
# BeautifulSoup/requests เข้าไม่ถึง — ต้องใช้ Playwright render JS ก่อน
#
# โครงสร้าง Shadow DOM:
#   <standalone-table>
#     #shadow-root
#       <table>
#         <tr><td>เชลล์ ฟิวเซฟ แก๊สโซฮอล์ E20</td><td>29.44</td></tr>
#         ...
#
# ชื่อน้ำมัน Shell:
#   เชลล์ ฟิวเซฟ แก๊สโซฮอล์ E20, เชลล์ ฟิวเซฟ แก๊สโซฮอล์ 91,
#   เชลล์ ฟิวเซฟ แก๊สโซฮอล์ 95, เชลล์ วี-เพาเวอร์ แก๊สโซฮอล์ 95,
#   เชลล์ ฟิวเซฟ ดีเซล, เชลล์ วี-เพาเวอร์ ดีเซล
# ══════════════════════════════════════════════════════════════════════════════
_SHELL_URL = "https://www.shell.co.th/th_th/customer/fuels-and-lubricants/fuels/fuel-price.html"

_SHELL_JS = """
() => {
    const el = document.querySelector('standalone-table');
    if (!el || !el.shadowRoot) return [];
    const rows = el.shadowRoot.querySelectorAll('tr');
    const data = [];
    rows.forEach(row => {
        const tds = row.querySelectorAll('td');
        if (tds.length >= 2) {
            const name = tds[0].innerText.trim().replace(/:$/, '').trim();
            const price = tds[1].innerText.trim();
            if (name && price) data.push({name, price});
        }
    });
    return data;
}
"""

def fetch_shell() -> dict:
    log.info("  [Shell] Playwright (Shadow DOM)...")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("  ⚠️  playwright ไม่ได้ติดตั้ง — ข้าม Shell (จะใช้ Kapook)")
        return {}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                user_agent=HEADERS["User-Agent"],
                extra_http_headers={"Accept-Language": "th-TH,th;q=0.9"}
            )
            page.goto(_SHELL_URL, wait_until="networkidle", timeout=30000)
            # รอ shadow DOM โหลด
            page.wait_for_selector("standalone-table", timeout=15000)
            raw = page.evaluate(_SHELL_JS)
            browser.close()

        if not raw:
            log.warning("  ⚠️  Shell Shadow DOM ว่าง — จะใช้ Kapook")
            return {}

        oils: dict = {}
        for item in raw:
            name = item["name"]
            price_text = item["price"]
            m = re.search(r"(\d{2,3}\.\d{2})", price_text)
            if not m:
                continue
            try:
                price = float(m.group(1))
            except ValueError:
                continue
            if price <= 0 or price > 200:
                continue
            family, order = get_family(name)
            key = slugify(name)
            if key and key not in oils:
                oils[key] = {"name": name, "price": price, "family": family, "order": order}
                log.info(f"    ✓ Shell | {name:38} {price:6.2f} [{family}]")

        log.info(f"  {'✅' if oils else '⚠️ '} Shell Playwright: {len(oils)} รายการ")
        return oils

    except Exception as e:
        log.warning(f"  ⚠️  Shell Playwright ล้มเหลว: {e} — จะใช้ Kapook")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE D: Caltex Official HTML
# URL  : caltex.com/th/motorists/products-and-services/fuel-prices.html
# โครงสร้าง: ราคาฝังตรงใน HTML
# ชื่อน้ำมัน: โกลด์ 95 เทครอน®, แก๊สโซฮอล์ 95 เทครอน®, แก๊สโซฮอล์ 91 เทครอน®,
#            แก๊สโซฮอล์ E20, ดีเซล เทครอน® ดี, พาวเวอร์ ดีเซล เทครอน® ดี
# ══════════════════════════════════════════════════════════════════════════════
_CALTEX_URL = "https://www.caltex.com/th/motorists/products-and-services/fuel-prices.html"

def fetch_caltex() -> dict:
    log.info("  [Caltex] Official HTML scrape...")
    try:
        r = requests.get(_CALTEX_URL, headers=HEADERS, timeout=20)
        r.raise_for_status()
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        oils: dict = {}

        # Layout: section ราคา — ชื่อน้ำมันและราคาอยู่ใน structure เดียวกัน
        # pattern ที่พบ: <img alt="ชื่อน้ำมัน"> + text "BHT 30.55"
        # หรือ text ที่มี pattern "ชื่อน้ำมัน\n\nBHT xx.xx"

        full_text = soup.get_text("\n", strip=True)

        # หา block ที่มี BHT price
        lines = [l.strip() for l in full_text.split("\n") if l.strip()]
        i = 0
        while i < len(lines):
            line = lines[i]
            bht_m = re.match(r"^BHT\s+(\d{2,3}\.\d{2})$", line)
            if bht_m:
                price = float(bht_m.group(1))
                # ชื่อน้ำมันอยู่ก่อนหน้า
                if i > 0:
                    name = lines[i - 1]
                    _try_add_caltex_oil(oils, name, price)
            i += 1

        # fallback: ใช้ alt text ของ img + ราคาถัดไป
        if not oils:
            for img in soup.find_all("img", alt=True):
                alt = img["alt"].strip()
                if not re.search(r"[\u0E00-\u0E7F]", alt):
                    continue
                # หาราคาจาก sibling หรือ next text
                parent = img.parent
                if parent:
                    text_after = parent.get_text(" ", strip=True)
                    m = re.search(r"(\d{2,3}\.\d{2})", text_after)
                    if m:
                        _try_add_caltex_oil(oils, alt, float(m.group(1)))

        log.info(f"  {'✅' if oils else '⚠️ '} Caltex HTML: {len(oils)} รายการ")
        return oils

    except Exception as e:
        log.warning(f"  ⚠️  Caltex HTML ล้มเหลว: {e}")
        return {}

def _try_add_caltex_oil(oils: dict, name: str, price: float):
    if not name or len(name) < 3:
        return
    if not re.search(r"[\u0E00-\u0E7F]|gasohol|diesel|caltex", name, re.I):
        return
    if price <= 0 or price > 200:
        return
    family, order = get_family(name)
    key = slugify(name)
    if key and key not in oils:
        oils[key] = {"name": name, "price": price, "family": family, "order": order}
        log.info(f"    ✓ Caltex | {name:38} {price:6.2f} [{family}]")


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE E: Kapook HTML — ข้อมูลจาก EPPO (กระทรวงพลังงาน)
# URL  : gasprice.kapook.com/gasprice.php
# ครอบคลุม: PTT, BCP, Shell, Caltex, IRPC, PT, Susco, Pure, Susco Dealers
# (ตัด Esso ออก — ไม่รวมใน brand map)
# โครงสร้าง HTML:
#   <h3>ราคาน้ำมัน Shell (shell)</h3>
#   <ul>
#     <li>แก๊สโซฮอล์ 95 <strong>31.85</strong></li>
#   </ul>
# ══════════════════════════════════════════════════════════════════════════════
# URL  : gasprice.kapook.com/gasprice.php
# ครอบคลุม: PTT, BCP, Shell, Caltex, Esso, IRPC, PT, Susco, Pure, Susco Dealers
# โครงสร้าง HTML:
#   <h3>ราคาน้ำมัน Shell (shell)</h3>
#   <ul>
#     <li>แก๊สโซฮอล์ 95 <strong>31.85</strong></li>
#   </ul>
# ══════════════════════════════════════════════════════════════════════════════
_KAPOOK_BRAND = {
    "ptt": "PTT",    "bcp": "BCP",      "shell": "Shell",
    "caltex": "Caltex",                  "irpc": "IRPC",
    "pt": "PT",      "susco": "Susco",  "suscodealers": "Susco Dealers",
    "pure": "Pure",
    # "esso": ถูกตัดออก
}

def fetch_kapook() -> dict[str, dict]:
    """คืน {brand: {oil_key: {...}}} สำหรับทุกแบรนด์ที่ Kapook มี"""
    log.info("  [Kapook] HTML scrape (EPPO data)...")
    try:
        r = requests.get(
            "http://gasprice.kapook.com/gasprice.php",
            headers=HEADERS,
            timeout=25,
        )
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        result: dict[str, dict] = {}

        for h3 in soup.find_all("h3"):
            h3_text = h3.get_text(strip=True)
            m = re.search(r"\((\w+)\)", h3_text)
            if not m:
                continue
            brand_key = m.group(1).lower()
            brand = _KAPOOK_BRAND.get(brand_key, brand_key.upper())
            result[brand] = {}

            ul = h3.find_next_sibling("ul") or h3.find_next("ul")
            if not ul:
                continue

            for li in ul.find_all("li"):
                price_el = li.find(["strong", "em", "b"])
                if not price_el:
                    continue
                price_str = price_el.get_text(strip=True)
                name = li.get_text(" ", strip=True).replace(price_str, "").strip(" *•-–")
                if not name:
                    continue
                try:
                    price = float(price_str.replace(",", "").strip())
                except ValueError:
                    continue
                if price <= 0:
                    continue
                family, order = get_family(name)
                key = slugify(name)
                result[brand][key] = {
                    "name": name, "price": price,
                    "family": family, "order": order,
                }
                log.info(f"    ✓ {brand:12} | {name:38} {price:6.2f} [{family}]")

        total = sum(len(v) for v in result.values())
        log.info(f"  {'✅' if result else '⚠️ '} Kapook: {len(result)} แบรนด์, {total} รายการ")
        return result

    except Exception as e:
        log.warning(f"  ⚠️  Kapook ล้มเหลว: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE F: chnwt.dev — Fallback สุดท้าย (scrape จาก Kapook เช่นกัน)
# ══════════════════════════════════════════════════════════════════════════════
_CHNWT_BRAND = {
    "ptt": "PTT", "shell": "Shell", "caltex": "Caltex",
    "esso": "Esso", "bcp": "BCP", "bangchak": "BCP",
    "pt": "PT", "susco": "Susco", "irpc": "IRPC",
    "pure": "Pure", "susco_dealers": "Susco Dealers",
}

def fetch_chnwt() -> dict[str, dict]:
    log.info("  [chnwt.dev] JSON fallback...")
    try:
        r = requests.get(
            "https://api.chnwt.dev/thai-oil-api/latest",
            headers=HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        stations = r.json().get("response", {}).get("stations", {})
        result: dict[str, dict] = {}

        for bk, oil_dict in stations.items():
            brand = _CHNWT_BRAND.get(bk.lower(), bk.upper())
            if not isinstance(oil_dict, dict):
                continue
            result[brand] = {}
            for oil_key, oil_data in oil_dict.items():
                if isinstance(oil_data, dict):
                    name = oil_data.get("name", oil_key.replace("_", " "))
                    price_raw = oil_data.get("price")
                else:
                    name, price_raw = oil_key.replace("_", " "), oil_data
                try:
                    price = float(str(price_raw).replace(",", ""))
                except (ValueError, TypeError):
                    continue
                if price <= 0:
                    continue
                family, order = get_family(name)
                key = slugify(name)
                result[brand][key] = {
                    "name": name, "price": price,
                    "family": family, "order": order,
                }

        total = sum(len(v) for v in result.values())
        log.info(f"  {'✅' if result else '❌'} chnwt.dev: {len(result)} แบรนด์, {total} รายการ")
        return result

    except Exception as e:
        log.warning(f"  ⚠️  chnwt.dev ล้มเหลว: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# Logic:
#   1. ดึง Kapook → ได้ทุกแบรนด์เป็น base (ข้อมูล EPPO, ยกเว้น Esso)
#   2. Override PTT ด้วย SOAP API (ถ้าสำเร็จ)
#   3. Override BCP ด้วย JSON API (ถ้าสำเร็จ) + ได้ราคาพรุ่งนี้
#   4. Override Shell ด้วย Official HTML (ถ้าสำเร็จ)
#   5. Override Caltex ด้วย Official HTML (ถ้าสำเร็จ)
#   6. ถ้า Kapook ล้มเหลว → fallback chnwt.dev ทั้งหมด
# ══════════════════════════════════════════════════════════════════════════════
def get_all_prices() -> dict[str, dict]:
    log.info("=" * 60)
    log.info(f"  Thai Oil Price Scraper — {TIMESTAMP}")
    log.info("=" * 60)
    log.info("\n📡 Step 1: ดึงข้อมูลฐาน (Kapook / EPPO)")

    base = fetch_kapook()

    if base:
        log.info("\n📌 Step 2: Override ด้วย Official APIs / HTML")

        ptt = fetch_ptt()
        if ptt:
            base["PTT"] = ptt
            log.info(f"  → PTT: ใช้ Official SOAP ({len(ptt)} รายการ)")
        else:
            log.info(f"  → PTT: คงข้อมูลจาก Kapook ({len(base.get('PTT', {}))} รายการ)")

        bcp = fetch_bcp()
        if bcp:
            base["BCP"] = bcp
            log.info(f"  → BCP: ใช้ Official JSON ({len(bcp)} รายการ + ราคาพรุ่งนี้)")
        else:
            log.info(f"  → BCP: คงข้อมูลจาก Kapook ({len(base.get('BCP', {}))} รายการ)")

        shell = fetch_shell()
        if shell:
            base["Shell"] = shell
            log.info(f"  → Shell: ใช้ Official HTML ({len(shell)} รายการ)")
        else:
            log.info(f"  → Shell: คงข้อมูลจาก Kapook ({len(base.get('Shell', {}))} รายการ)")

        caltex = fetch_caltex()
        if caltex:
            base["Caltex"] = caltex
            log.info(f"  → Caltex: ใช้ Official HTML ({len(caltex)} รายการ)")
        else:
            log.info(f"  → Caltex: คงข้อมูลจาก Kapook ({len(base.get('Caltex', {}))} รายการ)")

    else:
        log.warning("\n⚠️  Kapook ล้มเหลว — ใช้ chnwt.dev fallback")
        base = fetch_chnwt()

        if not base:
            log.error("❌ ทุก source ล้มเหลว")
            return {}

        # ตัด Esso ออก
        base.pop("Esso", None)

        # ยังพยายาม override ด้วย official APIs
        ptt = fetch_ptt()
        if ptt:
            base["PTT"] = ptt

        bcp = fetch_bcp()
        if bcp:
            base["BCP"] = bcp

        shell = fetch_shell()
        if shell:
            base["Shell"] = shell

        caltex = fetch_caltex()
        if caltex:
            base["Caltex"] = caltex

    total = sum(len(v) for v in base.values())
    log.info(f"\n✅ รวม: {len(base)} แบรนด์, {total} รายการ")
    return base


# ══════════════════════════════════════════════════════════════════════════════
# BUILD prices.json
# โครงสร้าง:
#   updated, date, brands,
#   oil_groups: [{family, oils:[{key, entries:{Brand:{name,price,price_tomorrow?}}}]}]
#   prices: {oil_key: {Brand: price}}  ← backward compat
# ══════════════════════════════════════════════════════════════════════════════
def build_output(raw: dict) -> dict:
    # รวบรวม metadata ของแต่ละ oil_key
    oil_meta: dict[str, dict] = {}
    for brand, oils in raw.items():
        for key, info in oils.items():
            if key not in oil_meta:
                oil_meta[key] = {"family": info["family"], "order": info["order"]}

    # จัดกลุ่ม family → oils
    fam_order: dict[str, int] = {}
    fam_oils: dict[str, list] = {}
    for key, meta in oil_meta.items():
        fam = meta["family"]
        fam_order.setdefault(fam, meta["order"])
        fam_oils.setdefault(fam, []).append(key)

    oil_groups = []
    for fam in sorted(fam_order, key=lambda f: fam_order[f]):
        group_oils = []
        for key in sorted(fam_oils[fam]):
            entries: dict = {}
            for brand, oils in raw.items():
                if key in oils:
                    e: dict = {
                        "name":  oils[key]["name"],
                        "price": oils[key]["price"],
                    }
                    if "price_tomorrow" in oils[key]:
                        e["price_tomorrow"] = oils[key]["price_tomorrow"]
                    entries[brand] = e
            if entries:
                group_oils.append({"key": key, "entries": entries})
        if group_oils:
            oil_groups.append({"family": fam, "oils": group_oils})

    # flat prices (backward compat)
    prices_flat = {
        key: {b: raw[b][key]["price"] for b in raw if key in raw[b]}
        for key in oil_meta
    }

    return {
        "updated":    TIMESTAMP,
        "date":       TODAY,
        "brands":     list(raw.keys()),
        "oil_groups": oil_groups,
        "prices":     prices_flat,
    }


def save_json(raw: dict):
    output = build_output(raw)

    # อัปเดต history (เก็บ 90 วัน)
    history: dict = {}
    if os.path.exists(HISTORY_JSON):
        try:
            with open(HISTORY_JSON, encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            pass
    history[TODAY] = output["prices"]
    history = {k: history[k] for k in sorted(history)[-90:]}

    with open(PRICES_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    with open(HISTORY_JSON, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    total = sum(len(g["oils"]) for g in output["oil_groups"])
    log.info(
        f"\n💾 บันทึก {PRICES_JSON}: "
        f"{len(output['brands'])} แบรนด์, "
        f"{total} รายการ, "
        f"{len(output['oil_groups'])} กลุ่ม"
    )


# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# Sheet 1 "ราคาล่าสุด"   — ตารางราคาปัจจุบันทุกแบรนด์ x ทุกประเภท
# Sheet 2 "ประวัติรายวัน" — append ทุกวัน (date, brand, name, family, price, price_tomorrow)
# ══════════════════════════════════════════════════════════════════════════════
def _gsheet_client():
    if not HAS_GSPREAD:
        raise ImportError("gspread ไม่ได้ติดตั้ง")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise ValueError("ไม่พบ GOOGLE_CREDENTIALS_JSON")
    return gspread.authorize(
        Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
    )

def _fmt_header(ws, ncols: int):
    try:
        ws.format(f"A1:{chr(64+min(ncols,26))}1", {
            "backgroundColor": {"red": 0.13, "green": 0.17, "blue": 0.28},
            "textFormat": {
                "foregroundColor": {"red": 0.9, "green": 0.75, "blue": 0.2},
                "bold": True, "fontSize": 11,
            },
            "horizontalAlignment": "CENTER",
        })
    except Exception:
        pass


def update_sheets(raw: dict):
    log.info("\n📊 กำลังอัปเดต Google Sheets...")
    try:
        gc = _gsheet_client()
        try:
            sh = gc.open(SHEET_NAME)
        except gspread.SpreadsheetNotFound:
            sh = gc.create(SHEET_NAME)
            sh.share(None, perm_type="anyone", role="reader")
            log.info(f"  📋 สร้าง Spreadsheet ใหม่: {SHEET_NAME}")

        output = build_output(raw)
        brands = output["brands"]

        # ── Sheet 1: ราคาล่าสุด ─────────────────────────────────────────────
        try:
            ws1 = sh.worksheet("ราคาล่าสุด")
            ws1.clear()
        except gspread.WorksheetNotFound:
            ws1 = sh.add_worksheet("ราคาล่าสุด", rows=200, cols=len(brands)+5)

        header = ["กลุ่ม", "ชื่อน้ำมัน"] + brands + ["BCP ราคาพรุ่งนี้", "อัปเดต"]
        rows = [header]
        for grp in output["oil_groups"]:
            for oil in grp["oils"]:
                # ชื่อน้ำมัน — ใช้จาก brand แรกที่มีข้อมูล
                display_name = next(
                    (oil["entries"][b]["name"] for b in brands if b in oil["entries"]),
                    oil["key"],
                )
                row: list = [grp["family"], display_name]
                for b in brands:
                    row.append(oil["entries"].get(b, {}).get("price", "—"))
                row.append(oil["entries"].get("BCP", {}).get("price_tomorrow", "—"))
                row.append(TIMESTAMP)
                rows.append(row)

        ws1.update(range_name="A1", values=rows)
        _fmt_header(ws1, len(header))
        log.info(f"  ✅ 'ราคาล่าสุด': {len(rows)-1} แถว")

        # ── Sheet 2: ประวัติรายวัน ───────────────────────────────────────────
        try:
            ws2 = sh.worksheet("ประวัติรายวัน")
        except gspread.WorksheetNotFound:
            ws2 = sh.add_worksheet("ประวัติรายวัน", rows=5000, cols=8)
            ws2.update(range_name="A1", values=[[
                "วันที่", "แบรนด์", "ชื่อน้ำมัน", "กลุ่ม",
                "ราคาวันนี้", "ราคาพรุ่งนี้", "oil_key", "source",
            ]])
            _fmt_header(ws2, 8)

        existing_dates = ws2.col_values(1)
        if TODAY not in existing_dates:
            new_rows = []
            for brand, oils in raw.items():
                for key, info in oils.items():
                    new_rows.append([
                        TODAY, brand, info["name"], info["family"],
                        info["price"],
                        info.get("price_tomorrow", info["price"]),
                        key,
                        "official_api" if brand == "PTT" else
                        "official_json" if brand == "BCP" else
                        "official_html" if brand in ("Shell", "Caltex") else
                        "kapook_eppo",
                    ])
            if new_rows:
                ws2.append_rows(new_rows)
                log.info(f"  ✅ 'ประวัติรายวัน': เพิ่ม {len(new_rows)} แถว")
        else:
            log.info("  ⏭️  ประวัติรายวัน: มีข้อมูลวันนี้แล้ว")

        log.info("✅ Google Sheets อัปเดตครบ")
    except Exception as e:
        log.error(f"❌ Google Sheets error: {e}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    raw = get_all_prices()
    if not raw:
        raise SystemExit(1)

    save_json(raw)

    if HAS_GSPREAD and os.environ.get("GOOGLE_CREDENTIALS_JSON"):
        update_sheets(raw)
    else:
        log.warning("⚠️  ข้าม Google Sheets (ไม่มี gspread หรือ GOOGLE_CREDENTIALS_JSON)")

    log.info("\n🎉 เสร็จสิ้น!")


if __name__ == "__main__":
    main()
