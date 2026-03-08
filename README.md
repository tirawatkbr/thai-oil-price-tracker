# 🛢️ ราคาน้ำมันไทย — Thailand Fuel Price Tracker

ระบบติดตามราคาน้ำมันไทยแบบ real-time อัปเดตอัตโนมัติทุกวัน ผ่าน GitHub Actions

**[🌐 ดูเว็บไซต์](https://tirawatkbr.github.io/thai-oil-tracker/)**

---

## แหล่งข้อมูล

| แบรนด์ | Source | วิธี |
|--------|--------|------|
| PTT | orapiweb.pttor.com | SOAP XML |
| BCP Bangchak | oil-price.bangchak.co.th | JSON REST |
| Shell | shell.co.th | Playwright (Shadow DOM) |
| Caltex | caltex.com/th | HTML Scrape |
| IRPC, PT, Susco, Pure | gasprice.kapook.com (EPPO) | HTML Scrape |
| Fallback | api.chnwt.dev | JSON REST |

## โครงสร้างไฟล์

```
thai-oil-tracker/
├── index.html              ← Dashboard (GitHub Pages)
├── prices.json             ← ราคาวันนี้ (auto-generated)
├── price_history.json      ← ประวัติ 90 วัน (auto-generated)
├── requirements.txt        ← Python dependencies
├── scripts/
│   └── scraper.py          ← Main scraper
└── .github/
    └── workflows/
        └── daily-oil-price.yml  ← GitHub Actions (รันทุกวัน 06:05 น.)
```

## ตั้งค่า GitHub Pages

1. ไปที่ **Settings → Pages**
2. Source: **Deploy from a branch**
3. Branch: **main** / root `(/)` 
4. Save

## Secrets (Optional — สำหรับ Google Sheets)

ไปที่ **Settings → Secrets and variables → Actions → New repository secret**

| Secret | ค่า |
|--------|-----|
| `GOOGLE_CREDENTIALS_JSON` | JSON content ของ Service Account key |
| `GOOGLE_SHEET_NAME` | ชื่อ Google Sheet (default: "Thai Oil Prices") |

## รันด้วยตัวเอง

```bash
pip install -r requirements.txt
playwright install chromium
python scripts/scraper.py
```

## prices.json Schema

```json
{
  "updated": "2024-03-05 06:05:00",
  "date": "2024-03-05",
  "brands": ["PTT", "BCP", "Shell", "Caltex", "IRPC", "PT", "Susco", "Pure", "Susco Dealers"],
  "oil_groups": [
    {
      "family": "g95",
      "oils": [
        {
          "key": "แก๊สโซฮอล์_95",
          "entries": {
            "PTT":   { "name": "แก๊สโซฮอล์ 95", "price": 30.55 },
            "Shell": { "name": "เชลล์ ฟิวเซฟ แก๊สโซฮอล์ 95", "price": 31.85 }
          }
        }
      ]
    }
  ],
  "prices": { "แก๊สโซฮอล์_95": { "PTT": 30.55 } }
}
```

---

Made with ❤️ + GitHub Actions + Python
