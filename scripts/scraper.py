"""
Thai Oil Price Scraper
======================
ดึงราคาน้ำมันจาก Official Sources รายแบรนด์:

  Brand          Source                                          Method
  ─────────────  ──────────────────────────────────────────────  ──────────
  PTT            orapiweb.pttor.com  (SOAP XML)                  SOAP
  BCP            oil-price.bangchak.co.th  (JSON REST)           REST
  Shell          shell.co.th  (Playwright Shadow DOM)            Browser
  Caltex         caltex.com/th  (HTML)                           Scrape
  IRPC/PT/Susco  gasprice.kapook.com  (EPPO mirror)             Scrape
  Pure           gasprice.kapook.com                             Scrape
  Fallback       api.chnwt.dev/thai-oil-api                      REST

Output:
  prices.json         — current prices (GitHub Pages reads this)
  price_history.json  — rolling 90-day history
  Google Sheets       — optional, via GOOGLE_CREDENTIALS_JSON secret
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ── Optional dependencies ─────────────────────────────────────────────────────
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

TZ        = ZoneInfo("Asia/Bangkok")
NOW       = datetime.now(TZ)
TODAY     = NOW.strftime("%Y-%m-%d")
TIMESTAMP = NOW.strftime("%Y-%m-%d %H:%M:%S")

PRICES_JSON  = "prices.json"
HISTORY_JSON = "price_history.json"
SHEET_NAME   = os.environ.get("GOOGLE_SHEET_NAME", "Thai Oil Prices")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "th-TH,th;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUEST_TIMEOUT = 20  # seconds

# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT SCHEMA
# ══════════════════════════════════════════════════════════════════════════════
#
#  prices.json structure:
#  {
#    "updated": "2024-03-05 06:05:00",
#    "date": "2024-03-05",
#    "brands": ["PTT", "BCP", "Shell", ...],
#    "oil_groups": [
#      {
#        "family": "g95",
#        "oils": [
#          {
#            "key": "แก๊สโซฮอล์_95",
#            "entries": {
#              "PTT":   {"name": "แก๊สโซฮอล์ 95", "price": 30.55},
#              "Shell": {"name": "เชลล์ ฟิวเซฟ แก๊สโซฮอล์ 95", "price": 31.85},
#              ...
#            }
#          }
#        ]
#      },
#      ...
#    ],
#    "prices": { "แก๊สโซฮอล์_95": {"PTT": 30.55, ...} }  ← flat legacy index
#  }

# Canonical family order for output
FAMILY_ORDER = [
    "benzene95",
    "g95_super",
    "g95_vpower",
    "g95_premium",
    "g95",
    "g91",
    "e20",
    "e85",
    "ngv",
    "diesel_vpower",
    "diesel_fuelsave",
    "diesel_premium",
    "diesel_b7",
    "diesel",
    "other",
]

ALL_BRANDS = [
    "PTT", "BCP", "Shell", "Caltex",
    "IRPC", "PT", "Susco", "Pure", "Susco Dealers",
]

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("oil-scraper")


# ══════════════════════════════════════════════════════════════════════════════
# DATA STORE
# ══════════════════════════════════════════════════════════════════════════════

class OilData:
    """
    Centralised in-memory store.

    Internal structure:
        store[family][oil_key][brand] = {
            "name": str,
            "price": float,
            "price_tomorrow": float | None,
        }
    """

    def __init__(self):
        self.store: dict[str, dict[str, dict[str, dict]]] = {}

    def add(
        self,
        family: str,
        oil_key: str,
        brand: str,
        name: str,
        price: float,
        price_tomorrow: float | None = None,
    ) -> None:
        self.store.setdefault(family, {}).setdefault(oil_key, {})
        entry: dict = {"name": name, "price": price}
        if price_tomorrow is not None:
            entry["price_tomorrow"] = price_tomorrow
        self.store[family][oil_key][brand] = entry

    def to_json_payload(self) -> dict:
        """Serialise to the canonical prices.json schema."""
        oil_groups: list[dict] = []

        for family in FAMILY_ORDER:
            oils_map = self.store.get(family, {})
            if not oils_map:
                continue
            oils = []
            for key, entries in oils_map.items():
                oils.append({"key": key, "entries": entries})
            oil_groups.append({"family": family, "oils": oils})

        # Collect brands actually present
        brands_seen: set[str] = set()
        for oils_map in self.store.values():
            for entries in oils_map.values():
                brands_seen.update(entries.keys())
        brands = [b for b in ALL_BRANDS if b in brands_seen]

        # Legacy flat prices index
        flat: dict[str, dict[str, float]] = {}
        for oils_map in self.store.values():
            for key, entries in oils_map.items():
                flat[key] = {b: v["price"] for b, v in entries.items()}

        return {
            "updated": TIMESTAMP,
            "date": TODAY,
            "brands": brands,
            "oil_groups": oil_groups,
            "prices": flat,
        }


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 1 — PTT  (SOAP XML)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ptt(db: OilData) -> bool:
    """
    PTT SOAP endpoint — returns today's retail prices for every PTT product.
    """
    url = "https://orapiweb.pttor.com/oilservice/OilPrice.asmx"
    soap_body = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <GetOilPrice2 xmlns="http://tempuri.org/">
      <Lang>2</Lang>
    </GetOilPrice2>
  </soap:Body>
</soap:Envelope>"""
    headers = {
        **HEADERS,
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": '"http://tempuri.org/GetOilPrice2"',
    }
    try:
        r = requests.post(url, data=soap_body.encode("utf-8"), headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns   = {"s": "http://schemas.xmlsoap.org/soap/envelope/", "t": "http://tempuri.org/"}

        # PTT XML  →  OilPriceItem with fields OilName, OilPrice, OilPriceTomorrow
        items = root.findall(".//{http://tempuri.org/}OilPriceItem")
        if not items:
            # try without namespace
            items = root.findall(".//OilPriceItem")

        count = 0
        for item in items:
            def txt(tag: str) -> str:
                el = item.find(f"{{http://tempuri.org/}}{tag}") or item.find(tag)
                return (el.text or "").strip() if el is not None else ""

            raw_name = txt("OilName") or txt("GoodsName") or txt("Name")
            raw_price = txt("OilPrice") or txt("Price") or txt("RetailPrice")
            raw_tomorrow = txt("OilPriceTomorrow") or txt("PriceTomorrow") or ""

            if not raw_name or not raw_price:
                continue
            try:
                price = float(raw_price.replace(",", ""))
            except ValueError:
                continue
            tomorrow: float | None = None
            try:
                tomorrow = float(raw_tomorrow.replace(",", ""))
            except ValueError:
                pass

            family, key = _ptt_classify(raw_name)
            if family:
                db.add(family, key, "PTT", raw_name, price, tomorrow)
                count += 1

        log.info(f"PTT SOAP  ✓  {count} products")
        return count > 0

    except Exception as exc:
        log.warning(f"PTT SOAP  ✗  {exc}")
        return False


def _ptt_classify(name: str) -> tuple[str, str]:
    """Map PTT product name → (family, key)."""
    n = name.lower()
    key = name.replace(" ", "_")

    if "เบนซิน" in n and "95" in n:
        return "benzene95", "เบนซิน_95"
    if "ซูเปอร์พาวเวอร์" in n or "super power" in n:
        return "g95_super", "ซูเปอร์พาวเวอร์_แก๊สโซฮอล์_95"
    if "95 พรีเมียม" in n or "premium 95" in n:
        return "g95_premium", "แก๊สโซฮอล์_95_พรีเมียม"
    if ("แก๊สโซฮอล์" in n or "gasohol" in n) and "95" in n:
        return "g95", "แก๊สโซฮอล์_95"
    if ("แก๊สโซฮอล์" in n or "gasohol" in n) and "91" in n:
        return "g91", "แก๊สโซฮอล์_91"
    if "e20" in n:
        return "e20", "แก๊สโซฮอล์_e20"
    if "e85" in n:
        return "e85", "แก๊สโซฮอล์_e85"
    if "ngv" in n:
        return "ngv", "แก๊ส_ngv"
    if "ดีเซลพรีเมียม" in n or "diesel premium" in n or "ดีเซล พรีเมียม" in n:
        return "diesel_premium", "ดีเซลพรีเมียม"
    if "ดีเซล" in n and "b7" in n:
        return "diesel_b7", "ดีเซล_b7"
    if "ดีเซล" in n:
        return "diesel", "ดีเซล"
    return "", ""


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 2 — BCP Bangchak  (JSON REST)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_bcp(db: OilData) -> bool:
    """
    BCP official JSON API — returns today + tomorrow prices.
    Endpoint: oil-price.bangchak.co.th/ApiOilPrice2/en
    """
    url = "https://oil-price.bangchak.co.th/ApiOilPrice2/en"
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        # data may be list or {"OilPrices": [...]}
        items = data if isinstance(data, list) else data.get("OilPrices", data.get("oilprices", []))

        count = 0
        for item in items:
            raw_name = (
                item.get("OilName") or item.get("Name") or
                item.get("product_name") or item.get("ProductName") or ""
            ).strip()
            raw_price = str(
                item.get("Price") or item.get("OilPrice") or
                item.get("price") or item.get("CurrentPrice") or ""
            ).strip()
            raw_tomorrow = str(
                item.get("PriceTomorrow") or item.get("TomorrowPrice") or
                item.get("price_tomorrow") or ""
            ).strip()

            if not raw_name or not raw_price:
                continue
            try:
                price = float(raw_price.replace(",", ""))
            except ValueError:
                continue
            tomorrow: float | None = None
            try:
                tomorrow = float(raw_tomorrow.replace(",", ""))
            except ValueError:
                pass

            family, key = _bcp_classify(raw_name)
            if family:
                db.add(family, key, "BCP", raw_name, price, tomorrow)
                count += 1

        log.info(f"BCP JSON  ✓  {count} products")
        return count > 0

    except Exception as exc:
        log.warning(f"BCP JSON  ✗  {exc}")
        return False


def _bcp_classify(name: str) -> tuple[str, str]:
    n = name.lower()
    key = name.replace(" ", "_")

    if "hi premium 97" in n or "premium 97" in n:
        return "g95_premium", "hi_premium_97_gasohol_95"
    if "gasohol 95" in n and "s evo" in n:
        return "g95", "gasohol_95_s_evo"
    if "gasohol 91" in n and "s evo" in n:
        return "g91", "gasohol_91_s_evo"
    if "gasohol e20" in n or "e20 s evo" in n:
        return "e20", "gasohol_e20_s_evo"
    if "gasohol e85" in n or "e85 s evo" in n:
        return "e85", "gasohol_e85_s_evo"
    if "hi premium diesel" in n:
        return "diesel_premium", "hi_premium_diesel_s"
    if "hi diesel" in n:
        return "diesel_b7", "hi_diesel_s"
    return "", ""


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 3 — Shell  (Playwright headless browser)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_shell(db: OilData) -> bool:
    """
    Shell Thailand uses Shadow DOM — requires Playwright to evaluate JS.
    URL: https://www.shell.co.th/th_TH/motorists/shell-fuels/app-fuel-prices.html
    """
    if not HAS_PLAYWRIGHT:
        log.warning("Shell      ✗  playwright not installed")
        return False

    url = "https://www.shell.co.th/th_TH/motorists/shell-fuels/app-fuel-prices.html"
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page    = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(3000)

            raw = page.evaluate("""() => {
                const results = [];
                const hosts = document.querySelectorAll('*');
                for (const el of hosts) {
                    if (el.shadowRoot) {
                        const items = el.shadowRoot.querySelectorAll(
                            '.fuel-price-item, [class*="price-row"], [class*="fuel-item"]'
                        );
                        items.forEach(item => {
                            const name  = item.querySelector(
                                '[class*="name"], [class*="title"], h3, h4, p'
                            )?.innerText?.trim();
                            const price = item.querySelector(
                                '[class*="price"], [class*="amount"], strong, span'
                            )?.innerText?.trim();
                            if (name && price) results.push({name, price});
                        });
                    }
                }
                return results;
            }""")
            browser.close()

        count = 0
        for item in (raw or []):
            raw_name  = (item.get("name")  or "").strip()
            raw_price = (item.get("price") or "").strip()
            m = re.search(r"[\d]+\.[\d]+", raw_price.replace(",", ""))
            if not raw_name or not m:
                continue
            try:
                price = float(m.group())
            except ValueError:
                continue
            family, key = _shell_classify(raw_name)
            if family:
                db.add(family, key, "Shell", raw_name, price)
                count += 1

        log.info(f"Shell      ✓  {count} products")
        return count > 0

    except Exception as exc:
        log.warning(f"Shell      ✗  {exc}")
        return False


def _shell_classify(name: str) -> tuple[str, str]:
    n = name.lower()
    key = name.replace(" ", "_")

    if "v-power" in n and "diesel" in n:
        return "diesel_vpower", "เชลล์_วี_เพาเวอร์_ดีเซล"
    if "fuelsave" in n and "diesel" in n or "ฟิวเซฟ" in n and "ดีเซล" in n:
        return "diesel_fuelsave", "เชลล์_ฟิวเซฟ_ดีเซล"
    if ("v-power" in n or "วี-เพาเวอร์" in n or "vpower" in n) and "95" in n:
        return "g95_vpower", "เชลล์_วี_เพาเวอร์_แก๊สโซฮอล์_95"
    if ("fuelsave" in n or "ฟิวเซฟ" in n) and "95" in n:
        return "g95", "เชลล์_ฟิวเซฟ_แก๊สโซฮอล์_95"
    if ("fuelsave" in n or "ฟิวเซฟ" in n) and "91" in n:
        return "g91", "เชลล์_ฟิวเซฟ_แก๊สโซฮอล์_91"
    if ("fuelsave" in n or "ฟิวเซฟ" in n) and "e20" in n:
        return "e20", "เชลล์_ฟิวเซฟ_แก๊สโซฮอล์_e20"
    return "", ""


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 4 — Caltex  (HTML scrape)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_caltex(db: OilData) -> bool:
    """
    Caltex Thailand fuel prices page.
    URL: https://www.caltex.com/th/th/motorists/fuel-prices.html
    """
    urls = [
        "https://www.caltex.com/th/th/motorists/fuel-prices.html",
        "https://www.caltex.com/th/th/motorists/fuels/fuel-prices.html",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

            count = 0
            # Look for price rows: element with fuel name + price number
            for row in soup.find_all(["tr", "li", "div"], class_=re.compile(r"price|fuel|product", re.I)):
                cells = row.find_all(["td", "span", "p", "div"])
                texts = [c.get_text(strip=True) for c in cells if c.get_text(strip=True)]
                if len(texts) < 2:
                    continue
                name = texts[0]
                for t in texts[1:]:
                    m = re.search(r"(\d+\.\d+)", t.replace(",", ""))
                    if m:
                        try:
                            price = float(m.group(1))
                            family, key = _caltex_classify(name)
                            if family:
                                db.add(family, key, "Caltex", name, price)
                                count += 1
                        except ValueError:
                            pass
                        break

            if count > 0:
                log.info(f"Caltex     ✓  {count} products  ({url})")
                return True

        except Exception as exc:
            log.debug(f"Caltex url failed: {url} — {exc}")

    log.warning("Caltex     ✗  all URLs failed")
    return False


def _caltex_classify(name: str) -> tuple[str, str]:
    n = name.lower()

    if "พาวเวอร์ ดีเซล" in n or "power diesel" in n:
        return "diesel_premium", "พาวเวอร์_ดีเซล_เทครอน_ดี"
    if "ดีเซล" in n or "diesel" in n:
        return "diesel_b7", "ดีเซล_เทครอน_ดี"
    if "โกลด์ 95" in n or "gold 95" in n:
        return "other", "โกลด์_95_เทครอน"
    if "95" in n:
        return "g95", "แก๊สโซฮอล์_95_เทครอน"
    if "91" in n:
        return "g91", "แก๊สโซฮอล์_91_เทครอน"
    if "e20" in n:
        return "e20", "แก๊สโซฮอล์_e20"
    return "", ""


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 5 — Kapook (EPPO mirror) — covers IRPC, PT, Susco, Pure + fallback
# ══════════════════════════════════════════════════════════════════════════════

_KAPOOK_BRAND_MAP = {
    "ptt": "PTT", "ปตท": "PTT",
    "shell": "Shell", "เชลล์": "Shell",
    "caltex": "Caltex", "คาลเท็กซ์": "Caltex", "คาเท็กซ์": "Caltex",
    "bcp": "BCP", "bangchak": "BCP", "บางจาก": "BCP",
    "irpc": "IRPC",
    "pt": "PT", "พีที": "PT",
    "susco": "Susco", "ซัสโก": "Susco",
    "pure": "Pure", "เพียว": "Pure",
    "susco dealers": "Susco Dealers",
}

_KAPOOK_OIL_MAP = {
    "เบนซิน 95": ("benzene95", "เบนซิน_95"),
    "แก๊สโซฮอล์ 95": ("g95", "แก๊สโซฮอล์_95"),
    "gasohol 95": ("g95", "แก๊สโซฮอล์_95"),
    "แก๊สโซฮอล์ 91": ("g91", "แก๊สโซฮอล์_91"),
    "gasohol 91": ("g91", "แก๊สโซฮอล์_91"),
    "e20": ("e20", "แก๊สโซฮอล์_e20"),
    "e85": ("e85", "แก๊สโซฮอล์_e85"),
    "ngv": ("ngv", "แก๊ส_ngv"),
    "ดีเซล b7": ("diesel_b7", "ดีเซล_b7"),
    "ดีเซลพรีเมียม": ("diesel_premium", "ดีเซลพรีเมียม"),
    "ดีเซล": ("diesel_b7", "ดีเซล_b7"),
}


def fetch_kapook(db: OilData, fill_brands: list[str] | None = None) -> bool:
    """
    Scrape gasprice.kapook.com — EPPO-sourced data, covers all brands.
    fill_brands: if given, only insert entries for those brands (don't overwrite official sources).
    """
    url = "https://gasprice.kapook.com/gasprice.php"
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "lxml")
        count = 0

        # Kapook layout: section per brand, table rows per oil type
        sections = soup.find_all("section", class_=re.compile(r"brand|station|oil", re.I))
        if not sections:
            # fallback: any div with brand logo + table
            sections = soup.find_all("div", class_=re.compile(r"brand|station", re.I))

        for section in sections:
            # Get brand name
            brand_el = section.find(class_=re.compile(r"brand.?name|logo.?text|station.?name", re.I))
            if not brand_el:
                brand_el = section.find(["h2", "h3", "h4", "strong"])
            if not brand_el:
                continue

            raw_brand = brand_el.get_text(strip=True).lower()
            brand = None
            for k, v in _KAPOOK_BRAND_MAP.items():
                if k in raw_brand:
                    brand = v
                    break
            if not brand:
                continue
            if fill_brands and brand not in fill_brands:
                continue

            # Get oil rows
            rows = section.find_all("tr") or section.find_all(class_=re.compile(r"oil.?row|price.?row", re.I))
            for row in rows:
                cells = row.find_all(["td", "span"])
                texts = [c.get_text(strip=True) for c in cells if c.get_text(strip=True)]
                if len(texts) < 2:
                    continue

                oil_name_raw = texts[0].lower().strip()
                family, key = None, None
                for k, v in _KAPOOK_OIL_MAP.items():
                    if k in oil_name_raw:
                        family, key = v
                        break
                if not family:
                    continue

                for t in texts[1:]:
                    m = re.search(r"(\d+\.\d+)", t.replace(",", ""))
                    if m:
                        try:
                            price = float(m.group(1))
                            db.add(family, key, brand, texts[0], price)
                            count += 1
                        except ValueError:
                            pass
                        break

        log.info(f"Kapook     ✓  {count} entries")
        return count > 0

    except Exception as exc:
        log.warning(f"Kapook     ✗  {exc}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 6 — Fallback REST API  (api.chnwt.dev)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_fallback_api(db: OilData) -> bool:
    """
    Community API mirroring EPPO data.
    Used only when all primary sources fail.
    """
    url = "https://api.chnwt.dev/thai-oil-api/latest"
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        count = 0
        for item in data.get("result", {}).get("price", []):
            oil_name = item.get("name", "")
            oil_lower = oil_name.lower()
            family, key = None, None
            for k, v in _KAPOOK_OIL_MAP.items():
                if k in oil_lower:
                    family, key = v
                    break
            if not family:
                continue

            for brand_raw, price_str in item.get("price", {}).items():
                brand = _KAPOOK_BRAND_MAP.get(brand_raw.lower(), brand_raw)
                try:
                    db.add(family, key, brand, oil_name, float(price_str))
                    count += 1
                except (ValueError, TypeError):
                    pass

        log.info(f"Fallback   ✓  {count} entries")
        return count > 0

    except Exception as exc:
        log.warning(f"Fallback   ✗  {exc}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def collect_all_prices() -> OilData:
    db = OilData()

    log.info("── Fetching official brand sources ──────────────────────────────")
    ptt_ok  = fetch_ptt(db)
    bcp_ok  = fetch_bcp(db)
    shell_ok = fetch_shell(db)
    caltex_ok = fetch_caltex(db)

    log.info("── Fetching Kapook (IRPC, PT, Susco, Pure, Susco Dealers) ───────")
    # Only fill brands not covered by official sources
    secondary_brands = ["IRPC", "PT", "Susco", "Pure", "Susco Dealers"]
    kapook_ok = fetch_kapook(db, fill_brands=secondary_brands)

    # Also let Kapook fill any brand where official scrape failed
    missing = []
    if not ptt_ok:    missing.append("PTT")
    if not bcp_ok:    missing.append("BCP")
    if not shell_ok:  missing.append("Shell")
    if not caltex_ok: missing.append("Caltex")
    if missing:
        log.info(f"── Kapook gap-fill for: {missing} ──────────────────────────────")
        fetch_kapook(db, fill_brands=missing)

    total = sum(
        len(entries)
        for oils in db.store.values()
        for entries in oils.values()
    )

    if total == 0:
        log.warning("── All sources failed — trying last-resort fallback API ─────")
        fetch_fallback_api(db)

    return db


# ══════════════════════════════════════════════════════════════════════════════
# PERSIST — JSON
# ══════════════════════════════════════════════════════════════════════════════

def save_json(db: OilData) -> None:
    payload = db.to_json_payload()

    with open(PRICES_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(f"Saved  {PRICES_JSON}  ({os.path.getsize(PRICES_JSON):,} bytes)")

    # Rolling 90-day history
    history: dict = {}
    if os.path.exists(HISTORY_JSON):
        with open(HISTORY_JSON, encoding="utf-8") as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = {}

    history[TODAY] = payload["prices"]
    kept = dict(list(sorted(history.items()))[-90:])

    with open(HISTORY_JSON, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)
    log.info(f"Saved  {HISTORY_JSON}  ({len(kept)} days of history)")


# ══════════════════════════════════════════════════════════════════════════════
# PERSIST — Google Sheets  (optional)
# ══════════════════════════════════════════════════════════════════════════════

def save_sheets(db: OilData) -> None:
    if not HAS_GSPREAD:
        log.warning("gspread not installed — skipping Google Sheets")
        return

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        log.warning("GOOGLE_CREDENTIALS_JSON not set — skipping Google Sheets")
        return

    try:
        creds_dict = json.loads(creds_json)
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc    = gspread.authorize(creds)

        try:
            sh = gc.open(SHEET_NAME)
        except gspread.SpreadsheetNotFound:
            sh = gc.create(SHEET_NAME)
            sh.share(None, perm_type="anyone", role="reader")
            log.info(f"Created Google Sheet: {SHEET_NAME}")

        _write_latest_sheet(sh, db)
        _write_history_sheet(sh, db)
        log.info("Google Sheets updated ✓")

    except Exception as exc:
        log.warning(f"Google Sheets error: {exc}")


def _write_latest_sheet(sh, db: OilData) -> None:
    payload = db.to_json_payload()
    brands  = payload["brands"]
    groups  = payload["oil_groups"]

    rows = [["ประเภทน้ำมัน", "ชื่อสินค้า"] + brands + ["อัปเดต"]]

    for grp in groups:
        for oil in grp["oils"]:
            entries = oil["entries"]
            display = next(iter(entries.values()), {}).get("name", oil["key"])
            row = [grp["family"], display]
            for b in brands:
                ent = entries.get(b)
                row.append(ent["price"] if ent else "—")
            row.append(TIMESTAMP)
            rows.append(row)

    try:
        ws = sh.worksheet("ราคาล่าสุด")
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("ราคาล่าสุด", rows=200, cols=30)

    ws.update("A1", rows)
    _fmt_header(ws, len(rows[0]))


def _write_history_sheet(sh, db: OilData) -> None:
    payload = db.to_json_payload()
    brands  = payload["brands"]

    try:
        ws = sh.worksheet("ประวัติราคา")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("ประวัติราคา", rows=1000, cols=50)
        header = ["วันที่", "ประเภท", "สินค้า"] + brands
        ws.update("A1", [header])
        _fmt_header(ws, len(header))

    existing_dates = ws.col_values(1)
    if TODAY in existing_dates:
        log.info("History sheet: today already recorded, skipping")
        return

    new_rows = []
    for grp in payload["oil_groups"]:
        for oil in grp["oils"]:
            entries = oil["entries"]
            display = next(iter(entries.values()), {}).get("name", oil["key"])
            row = [TODAY, grp["family"], display]
            for b in brands:
                ent = entries.get(b)
                row.append(ent["price"] if ent else "")
            new_rows.append(row)
            time.sleep(0.1)

    if new_rows:
        ws.append_rows(new_rows)


def _fmt_header(ws, cols: int) -> None:
    try:
        ws.format(
            f"A1:{chr(64 + min(cols, 26))}1",
            {
                "backgroundColor": {"red": 0.13, "green": 0.17, "blue": 0.28},
                "textFormat": {
                    "foregroundColor": {"red": 0.9, "green": 0.75, "blue": 0.2},
                    "bold": True,
                    "fontSize": 11,
                },
                "horizontalAlignment": "CENTER",
            },
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(db: OilData) -> None:
    payload = db.to_json_payload()
    total = sum(len(o["entries"]) for g in payload["oil_groups"] for o in g["oils"])

    log.info("")
    log.info("═" * 55)
    log.info(f"  ✅  Collected {total} price entries across {len(payload['brands'])} brands")
    log.info(f"  📅  Date: {TODAY}   ⏰  {TIMESTAMP[-8:]}")
    log.info("═" * 55)

    for grp in payload["oil_groups"]:
        for oil in grp["oils"]:
            if not oil["entries"]:
                continue
            sample = "  ".join(
                f"{b}: ฿{v['price']:.2f}"
                for b, v in list(oil["entries"].items())[:4]
            )
            label = next(iter(oil["entries"].values()))["name"]
            log.info(f"  {label:<36} {sample}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("═" * 55)
    log.info(f"  🛢️  Thai Oil Price Scraper  —  {TIMESTAMP}")
    log.info("═" * 55)

    db = collect_all_prices()

    total = sum(
        len(entries)
        for oils in db.store.values()
        for entries in oils.values()
    )
    if total == 0:
        log.error("No price data collected — aborting")
        raise SystemExit(1)

    print_summary(db)
    save_json(db)
    save_sheets(db)

    log.info("")
    log.info("  🎉  Done!")


if __name__ == "__main__":
    main()
