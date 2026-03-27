#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Device News Scraper - Audio Electronics New Products Monitor
Crawls semiconductor manufacturer websites daily and generates HTML reports.
Working directory: D:\00_GoogleDrive\01_Brise_Audio\02_Electric\06_新製品情報\news
"""

import os
import re
import json
import sys
import asyncio
import logging
from datetime import datetime, date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
REPORTS_DIR = BASE_DIR / "docs"
DB_PATH = BASE_DIR / "known_products.json"
REPORTS_DIR.mkdir(exist_ok=True)

# ─── Logging ──────────────────────────────────────────────────────────────────
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.stream.reconfigure(encoding="utf-8", errors="replace")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        _stream_handler,
        logging.FileHandler(BASE_DIR / "scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── Audio-relevant category keywords ─────────────────────────────────────────
AUDIO_INCLUDE_PATTERNS = [
    r"op.?amp",
    r"operational.?amp",
    r"instrument(ation)?.?amp",
    r"audio.?amp",
    r"power.?amp",
    r"fully.?differential",
    r"current.?sense",
    r"comparator",
    r"voltage.?ref",
    r"ldo",
    r"linear.?reg",
    r"dc.?dc",
    r"\bbuck\b",
    r"\bboost\b",
    r"switching.?reg",
    r"isolat",
    r"\bclock\b",
    r"oscillat",
    r"ocxo",
    r"tcxo",
    r"\bdac\b",
    r"\badc\b",
    r"mosfet",
    r"\bgan\b",
    r"gate.?driv",
    r"transistor",
    r"\bjfet\b",
    r"bipolar",
    r"load.?switch",
    r"charge.?pump",
    r"battery.?charg",
    r"power.?manag",
    r"voltage.?detect",
    r"\bpll\b",
    r"\bfilter\b",
    r"inductor",
    r"\baudio\b",
    r"high.?speed.?amp",
    r"diff(erence)?.?amp",
    r"\bina\d",
    r"power.?module",
    r"step.?down",
    r"step.?up",
    r"\bsic\b",
    r"data.?convert",
    r"analog.?to.?digital",
    r"digital.?to.?analog",
    r"precision.?amp",
    r"voltage.?regul",
    r"shunt.?ref",
    r"level.?shift", r"level.?translat", r"レベル変換", r"レベルシフタ",
    r"buffer", r"バッファ",
    r"analog.?switch", r"アナログスイッチ",
    r"diode", r"ダイオード", r"tvs",
    r"power.?monitor", r"電源監視", r"電圧監視",
    r"ac.?dc", r"AC-DC",
    r"codec", r"コーデック",
    r"ideal.?diode", r"理想ダイオード",
    r"half.?bridge", r"ハーフブリッジ", r"ハフブリッジ",
    r"transform", r"トランス",
    r"resistor", r"抵抗",
    r"esd.?protect", r"ESD保護",
    r"multiplexer", r"mux\b", r"マルチプレクサ",
    r"photo.?relay", r"フォトリレー", r"固体リレー",
    r"mems.?switch", r"MEMSスイッチ",
    r"pd.?control", r"PDコントローラ",
    r"crystal", r"水晶",
    r"差動",
    r"オペアンプ",
    r"アンプ",
    r"電源",
    r"レギュレータ",
    r"発振",
    r"トランジスタ",
    r"コンバータ",
    r"アイソレータ",
    r"オシレータ",
    r"PLLクロック",
    r"スイッチングレギュレータ",
    r"負荷スイッチ",
    r"チャージポンプ",
    r"電池充電",
    r"電圧検出",
    r"SiC",
]

AUDIO_EXCLUDE_PATTERNS = [
    r"motor.?driv",
    r"\bgnss\b",
    r"\bgps\b",
    r"bluetooth",
    r"\bwifi\b",
    r"zigbee",
    r"lora",
    r"wireless",
    r"microcontroller",
    r"\bmcu\b",
    r"\bfpga\b",
    r"watchdog",
    r"can.?transceiv",
    r"lin.?transceiv",
    r"ethernet",
    r"led.?driv",
    r"\bdisplay\b",
    r"\blcd\b",
    r"temperature.?sensor",
    r"humidity",
    r"pressure.?sensor",
    r"accelero",
    r"\bgyro\b",
    r"touch.?sens",
    r"モータ",
    r"無線",
    r"タッチ",
    r"LED.*ドライバ",
    r"Backlight",
    r"backlight",
]

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

# Some sites block Chrome UA; use minimal UA for those
HTTP_HEADERS_SIMPLE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def is_audio_relevant(text: str) -> bool:
    t = text.lower()
    for pat in AUDIO_EXCLUDE_PATTERNS:
        if re.search(pat, t, re.IGNORECASE):
            return False
    for pat in AUDIO_INCLUDE_PATTERNS:
        if re.search(pat, t, re.IGNORECASE):
            return True
    return False


def fetch(url: str, timeout: int = 25,
          headers: dict | None = None) -> requests.Response | None:
    try:
        r = requests.get(url, headers=headers or HTTP_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f"Fetch failed {url}: {e}")
        return None


def clean_text(s: str) -> str:
    """Strip HTML entities and whitespace."""
    import html
    return re.sub(r"\s+", " ", html.unescape(s or "")).strip()


def make_product(manufacturer: str, part: str, description: str,
                 category: str, package: str, url: str,
                 announced: str = "") -> dict:
    return {
        "manufacturer": manufacturer,
        "part": str(part or ""),
        "description": str(description or ""),
        "category": str(category or ""),
        "package": str(package or ""),
        "url": str(url or ""),
        "announced": str(announced or ""),
        "key": f"{manufacturer}::{part}",
    }


# ─── TI Scraper ───────────────────────────────────────────────────────────────

def scrape_ti() -> list[dict]:
    """Fetch TI new products via their internal selection model API (938 products, last 12 months)."""
    log.info("Scraping TI...")
    url = (
        "https://www.ti.com/selectionmodel/api/gpn/result-list"
        "?destinationId=999999&destinationType=NP&mode=parametric&locale=en-US"
    )
    r = fetch(url)
    if not r:
        return []

    try:
        data = r.json()
    except Exception as e:
        log.error(f"TI JSON parse error: {e}")
        return []

    products = []
    for item in data.get("results", []):
        part = item.get("genericPartNumber", "")
        loc = item.get("localization", {}).get("en-US", {})
        title = loc.get("title", "")
        sub_families = [sf["name"] for sf in loc.get("subFamilies", [])]
        silo_families = [sf["name"] for sf in loc.get("siloFamilies", [])]
        category = " / ".join(sub_families) if sub_families else " / ".join(silo_families)

        package = ""
        for param in item.get("paramList", []):
            if param.get("name") == "Package type":
                vals = param.get("value", {}).get("base", [])
                if vals:
                    package = vals[0]
                break

        title = clean_text(title)
        if not is_audio_relevant(f"{title} {category}"):
            continue

        products.append(make_product(
            manufacturer="TI",
            part=part,
            description=title,
            category=category,
            package=package,
            url=f"https://www.ti.com/product/{part}",
        ))

    log.info(f"TI: {len(products)} audio-relevant products")
    return products


# ─── ADI Scraper ──────────────────────────────────────────────────────────────

def scrape_adi() -> list[dict]:
    """Fetch ADI new products from their static HTML listing page."""
    log.info("Scraping ADI...")
    r = fetch("https://www.analog.com/en/new-products.html",
              timeout=30, headers=HTTP_HEADERS_SIMPLE)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    products = []
    seen_parts = set()

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/en/products/" not in href:
            continue
        part = link.get_text(strip=True)
        if not part or part in seen_parts:
            continue
        seen_parts.add(part)

        if not href.startswith("http"):
            href = "https://www.analog.com" + href

        description = ""
        node = link
        for _ in range(6):
            node = node.parent
            if not node:
                break
            txt = node.get_text(separator=" | ", strip=True)
            if len(txt) > 60:
                parts = txt.split(" | ")
                description = parts[-1].strip() if len(parts) > 1 else txt.strip()
                description = description[:200]
                break

        if not is_audio_relevant(f"{part} {description}"):
            continue

        products.append(make_product(
            manufacturer="ADI",
            part=part,
            description=description,
            category="",
            package="",
            url=href,
        ))

    log.info(f"ADI: {len(products)} audio-relevant products")
    return products


# ─── Nisshinbo Scraper ────────────────────────────────────────────────────────

def scrape_nisshinbo() -> list[dict]:
    """Fetch Nisshinbo new products from their English new-products page."""
    log.info("Scraping Nisshinbo...")
    r = fetch("https://www.nisshinbo-microdevices.co.jp/en/about/new_products/")
    if not r:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    main = soup.find("main")
    if not main:
        return []

    products = []
    text = main.get_text(separator="\n", strip=True)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    i = 0
    while i < len(lines):
        date_match = re.match(
            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4}",
            lines[i], re.IGNORECASE
        )
        if date_match and i + 3 < len(lines):
            announced = lines[i]
            j = i + 1
            while j < len(lines) and re.match(r"new\s+(product|ic|item)", lines[j], re.IGNORECASE):
                j += 1
            if j < len(lines):
                part = lines[j]
                description = lines[j + 1] if j + 1 < len(lines) else ""

                if is_audio_relevant(f"{part} {description}"):
                    # Find product URL
                    prod_url = "https://www.nisshinbo-microdevices.co.jp/en/about/new_products/"
                    base_part = part.split("/")[0].strip()
                    for a_tag in main.find_all("a", href=True):
                        if base_part and base_part in a_tag.get_text():
                            href = a_tag["href"]
                            if not href.startswith("http"):
                                href = "https://www.nisshinbo-microdevices.co.jp" + href
                            prod_url = href
                            break

                    products.append(make_product(
                        manufacturer="Nisshinbo",
                        part=part,
                        description=description,
                        category="",
                        package="",
                        url=prod_url,
                        announced=announced,
                    ))
        i += 1

    log.info(f"Nisshinbo: {len(products)} audio-relevant products")
    return products


# ─── Sanken Scraper ───────────────────────────────────────────────────────────

def scrape_sanken() -> list[dict]:
    """Fetch Sanken new products from their Japanese new-products page."""
    log.info("Scraping Sanken...")
    r = fetch("https://www.semicon.sanken-ele.co.jp/newproduct/")
    if not r:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    main = soup.find("main")
    if not main:
        return []

    products = []
    for wrapper in main.find_all("div", class_="img-cap-txt-wrapper"):
        txt_div = wrapper.find("div", class_="txt")
        if not txt_div:
            continue

        ttl = txt_div.find("p", class_="ttl")
        title = ttl.get_text(strip=True) if ttl else ""

        date_str = ""
        for p in txt_div.find_all("p"):
            ptext = p.get_text(strip=True)
            if re.match(r"20\d\d/\d\d?/\d\d?", ptext):
                date_str = ptext
                break

        lead = txt_div.find("p", class_="news_lead")
        category = ""
        if lead:
            cat_text = lead.get_text(strip=True)
            cat_match = re.search(r"製品カテゴリ：\s*(.*)", cat_text)
            category = cat_match.group(1).strip() if cat_match else cat_text

        prod_url = "https://www.semicon.sanken-ele.co.jp/newproduct/"
        for a_tag in wrapper.find_all("a", href=True):
            href = a_tag["href"]
            if "detail" in href or "ctrl/product" in href:
                prod_url = ("https://www.semicon.sanken-ele.co.jp" + href
                            if href.startswith("/") else href)
                break

        part_match = re.search(r"[【\[]([^\]】]+)[】\]]", title)
        part = part_match.group(1) if part_match else re.sub(r"\s+", " ", title[:40])

        if not is_audio_relevant(f"{title} {category}"):
            continue

        products.append(make_product(
            manufacturer="Sanken",
            part=part,
            description=title,
            category=category,
            package="",
            url=prod_url,
            announced=date_str,
        ))

    log.info(f"Sanken: {len(products)} audio-relevant products")
    return products


# ─── Torex Scraper ────────────────────────────────────────────────────────────

# Torex audio-relevant categories (Japanese names from their series table)
TOREX_AUDIO_CATS = {
    "PLLクロックジェネレータ",
    "電圧レギュレータ",
    "DC/DCコンバータ",
    "負荷スイッチ",
    "チャージポンプ",
    "電池充電IC",
    "MOSFET",
    "パワーMOSFET",
    "SiC-SBD",
}

def scrape_torex() -> list[dict]:
    """Scrape Torex series list, filtering for products with NEW status."""
    log.info("Scraping Torex...")
    r = fetch("https://product.torexsemi.com/ja/series",
              headers=HTTP_HEADERS_SIMPLE)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    main = soup.find("main")
    if not main:
        return []

    table = main.find("table")
    if not table:
        return []

    products = []
    rows = table.find_all("tr")
    for row in rows[1:]:  # skip header
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        series_cell = cells[0]
        series_link = series_cell.find("a", href=True)
        part = series_cell.get_text(strip=True)
        category = cells[1].get_text(strip=True)
        features = cells[2].get_text(strip=True)[:150]
        status = cells[4].get_text(strip=True)
        package = cells[5].get_text(strip=True) if len(cells) > 5 else ""

        # Only include products with "新規" in status (new recommended)
        if "新規" not in status and "NEW" not in status.upper():
            continue

        # Filter by audio-relevant categories
        if category not in TOREX_AUDIO_CATS and not is_audio_relevant(f"{category} {features}"):
            continue

        url = ""
        if series_link:
            href = series_link["href"]
            url = href if href.startswith("http") else "https://product.torexsemi.com" + href

        products.append(make_product(
            manufacturer="Torex",
            part=part,
            description=features,
            category=category,
            package=package,
            url=url.strip(),
        ))

    log.info(f"Torex: {len(products)} new audio-relevant products")
    return products


# ─── Rohm Scraper (Playwright) ────────────────────────────────────────────────

async def _scrape_rohm_async() -> list[dict]:
    """Fetch Rohm new products using Playwright (JS-rendered page)."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("Playwright not installed, skipping Rohm")
        return []

    products = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto("https://www.rohm.com/new-products-listing",
                            wait_until="networkidle", timeout=30000)

            rows = await page.evaluate(r"""() => {
                const rows = document.querySelectorAll('table tr');
                const result = [];
                rows.forEach(row => {
                    const cells = Array.from(row.querySelectorAll('td'));
                    if (cells.length < 4) return;
                    const links = row.querySelectorAll('a[href*="/products/"]');
                    const productLinks = Array.from(links)
                        .map(a => a.href)
                        .filter(h => !h.endsWith('.pdf'));
                    result.push({
                        part: cells[0].innerText.trim(),
                        group: cells[1].innerText.trim(),
                        name: cells[2].innerText.trim(),
                        status: cells[3].innerText.trim(),
                        date: cells[4] ? cells[4].innerText.trim() : '',
                        url: productLinks[0] || ''
                    });
                });
                return result;
            }""")
        finally:
            await browser.close()

    for item in rows:
        part = item.get("part", "")
        group = item.get("group", "")
        name = item.get("name", "")
        combined = f"{part} {group} {name}"
        if not is_audio_relevant(combined):
            continue
        products.append(make_product(
            manufacturer="Rohm",
            part=part,
            description=name,
            category=group,
            package="",
            url=item.get("url", ""),
            announced=item.get("date", ""),
        ))
    return products


def scrape_rohm() -> list[dict]:
    """Sync wrapper for async Rohm scraper."""
    log.info("Scraping Rohm...")
    try:
        products = asyncio.run(_scrape_rohm_async())
        log.info(f"Rohm: {len(products)} audio-relevant products")
        return products
    except Exception as e:
        log.error(f"Rohm scraper failed: {e}")
        return []


# ─── Renesas Scraper (Playwright — press releases) ────────────────────────────

async def _scrape_renesas_async() -> list[dict]:
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(
            "https://www.renesas.com/en/about/newsroom/news-search?news_type=press_release",
            wait_until="networkidle", timeout=30_000,
        )
        items = await page.evaluate(r"""() => {
            const results = [];
            document.querySelectorAll('a').forEach(a => {
                const text = a.innerText.trim();
                const href = a.href;
                if (text.length > 30 && /renesas/i.test(text) && href.includes('/newsroom/')) {
                    results.push({title: text, url: href});
                }
            });
            return results;
        }""")
        await browser.close()
    products = []
    for item in items:
        title = clean_text(item.get("title", ""))
        url = item.get("url", "")
        if not is_audio_relevant(title):
            continue
        # Extract part number from title if possible (e.g., "RH850/U2C", "TP65B110HRU")
        import re as _re
        m = _re.search(r'\b([A-Z]{1,3}\d{2,}[A-Z0-9/]*)\b', title)
        part = m.group(1) if m else title[:40]
        products.append(make_product(
            manufacturer="Renesas",
            part=part,
            description=title,
            category="",
            package="",
            url=url,
        ))
    return products


def scrape_renesas() -> list[dict]:
    log.info("Scraping Renesas...")
    try:
        products = asyncio.run(_scrape_renesas_async())
        log.info(f"Renesas: {len(products)} audio-relevant products")
        return products
    except Exception as e:
        log.error(f"Renesas scraper failed: {e}")
        return []


# ─── Database ─────────────────────────────────────────────────────────────────

BACKUP_DIR = BASE_DIR / "backups"
BACKUP_KEEP_DAYS = 30


def load_db() -> dict:
    if DB_PATH.exists():
        try:
            with open(DB_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.error(f"DB load failed ({e}), trying latest backup...")
            # Fall back to most recent backup
            if BACKUP_DIR.exists():
                backups = sorted(BACKUP_DIR.glob("known_products_*.json"), reverse=True)
                for bk in backups:
                    try:
                        with open(bk, encoding="utf-8") as f:
                            data = json.load(f)
                        log.warning(f"Restored from backup: {bk.name}")
                        return data
                    except Exception:
                        continue
    return {"known_keys": {}, "last_run": ""}


def save_db(db: dict):
    BACKUP_DIR.mkdir(exist_ok=True)

    # 1. Atomic write: write to .tmp then rename
    tmp = DB_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    tmp.replace(DB_PATH)

    # 2. Daily backup copy (overwrites same-day backup if run multiple times)
    today_str = date.today().isoformat()
    backup_path = BACKUP_DIR / f"known_products_{today_str}.json"
    import shutil
    shutil.copy2(DB_PATH, backup_path)

    # 3. Prune old backups (keep last BACKUP_KEEP_DAYS)
    all_backups = sorted(BACKUP_DIR.glob("known_products_*.json"), reverse=True)
    for old in all_backups[BACKUP_KEEP_DAYS:]:
        old.unlink()
        log.debug(f"Pruned old backup: {old.name}")


def update_db(scraped: list[dict], db: dict) -> set[str]:
    """Merge scraped products into DB. Return keys of newly discovered products."""
    known = db.setdefault("known_keys", {})
    products_store = db.setdefault("products", {})
    today = date.today().isoformat()
    new_keys = set()

    for p in scraped:
        key = p["key"]
        if key not in known:
            known[key] = today
            new_keys.add(key)
        # Set found_date from first-seen record (strip time if present)
        p["found_date"] = known[key][:10]
        if not p.get("announced"):
            p["announced"] = None
        products_store[key] = p

    return new_keys


# ─── Translation (EN→JA with cache) ──────────────────────────────────────────

TRANS_CACHE_PATH = BASE_DIR / "translations.json"
_TRANS_CACHE: dict | None = None


def _load_trans_cache() -> dict:
    global _TRANS_CACHE
    if _TRANS_CACHE is None:
        if TRANS_CACHE_PATH.exists():
            try:
                with open(TRANS_CACHE_PATH, encoding="utf-8") as f:
                    _TRANS_CACHE = json.load(f)
            except Exception:
                _TRANS_CACHE = {}
        else:
            _TRANS_CACHE = {}
    return _TRANS_CACHE


def _save_trans_cache():
    if _TRANS_CACHE is not None:
        with open(TRANS_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(_TRANS_CACHE, f, ensure_ascii=False, indent=2)


def _is_japanese(s: str) -> bool:
    return bool(re.search(r"[\u3040-\u30ff\u4e00-\u9fff]", s))


def translate_descriptions(products: list[dict]) -> list[dict]:
    """Translate English product descriptions to Japanese (with cache)."""
    try:
        from deep_translator import GoogleTranslator
    except ImportError:
        log.warning("deep-translator not installed; descriptions kept in English")
        return products

    cache = _load_trans_cache()
    to_translate = []  # (index, text)

    for i, p in enumerate(products):
        desc = p.get("description", "")
        if not desc or _is_japanese(desc) or len(desc) < 8:
            continue
        if desc in cache:
            p["description"] = cache[desc]
        else:
            to_translate.append((i, desc))

    if to_translate:
        log.info(f"Translating {len(to_translate)} descriptions EN→JA ...")
        translator = GoogleTranslator(source="en", target="ja")
        # Batch in chunks of 50 (Google limit)
        chunk = 50
        for start in range(0, len(to_translate), chunk):
            batch = to_translate[start:start + chunk]
            texts = [t for _, t in batch]
            try:
                results = translator.translate_batch(texts)
                for (idx, orig), ja in zip(batch, results):
                    if ja:
                        cache[orig] = ja
                        products[idx]["description"] = ja
            except Exception as e:
                log.warning(f"Translation batch failed: {e}")

        _save_trans_cache()
        log.info("Translation done.")

    return products


# ─── Genre classification ─────────────────────────────────────────────────────

# Ordered list: (genre_label, [match_patterns])
GENRE_RULES: list[tuple[str, list[str]]] = [
    ("オペアンプ / 計装アンプ", [
        r"op.?amp", r"operational.?amp", r"instrument(ation)?.?amp",
        r"fully.?differential", r"current.?sense", r"diff.?amp",
        r"audio.?amp.*prec", r"precision.*amp",
        r"オペアンプ", r"計装アンプ", r"差動アンプ",
    ]),
    ("パワーアンプ / ゲートドライバ", [
        r"power.?amp", r"class.?[abcd].*amp", r"gate.?driv", r"パワーアンプ",
        r"ゲートドライバ", r"audio.*amplif",
    ]),
    ("クロック / オシレータ", [
        r"clock", r"oscillat", r"ocxo", r"tcxo", r"xo\b", r"\bpll\b",
        r"timing", r"jitter", r"クロック", r"発振", r"オシレータ", r"PLLクロック",
    ]),
    ("アイソレータ / デジタルアイソレータ", [
        r"isolat", r"アイソレータ",
    ]),
    ("DAC / ADC", [
        r"\bdac\b", r"\badc\b", r"data.?convert", r"analog.?to.?digital",
        r"digital.?to.?analog", r"変換器",
    ]),
    ("電源IC - LDO / リニアレギュレータ", [
        r"\bldo\b", r"low.?dropout", r"linear.?reg", r"LDOレギュレータ",
        r"電圧レギュレータ",
    ]),
    ("電源IC - DC-DC / スイッチング", [
        r"dc.?dc", r"\bbuck\b", r"\bboost\b", r"step.?down", r"step.?up",
        r"switching.?reg", r"power.?module", r"DC/DCコンバータ",
        r"降圧", r"昇圧", r"スイッチングレギュレータ",
    ]),
    ("電源IC - 充電・電池管理", [
        r"battery.?charg", r"charger", r"charge.?manag", r"power.?path",
        r"電池充電", r"充電管理", r"電池充電IC",
    ]),
    ("負荷スイッチ / チャージポンプ", [
        r"load.?switch", r"charge.?pump", r"負荷スイッチ", r"チャージポンプ",
    ]),
    ("コンパレータ / 電圧検出", [
        r"comparator", r"voltage.?detect", r"voltage.?monitor",
        r"コンパレータ", r"電圧検出",
    ]),
    ("電圧リファレンス", [
        r"voltage.?ref", r"shunt.?ref", r"reference",
        r"電圧リファレンス",
    ]),
    ("トランジスタ / MOSFET / GaN", [
        r"transistor", r"mosfet", r"\bgan\b", r"jfet", r"bipolar",
        r"field.?effect", r"sic", r"power.?device",
        r"トランジスタ", r"MOSFET", r"SiC", r"過渡電圧サプレッサ",
    ]),
    ("フィルタ / パッシブ部品", [
        r"filter", r"inductor", r"ferrite", r"capacitor",
        r"フィルタ", r"インダクタ", r"コンデンサ",
    ]),
]


def classify_genre(p: dict) -> str:
    text = f"{p.get('category','')} {p.get('description','')} {p.get('part','')}".lower()
    for genre, patterns in GENRE_RULES:
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                return genre
    return "その他"

# Keywords that flag a product as especially audio-relevant (★ highlight)
_AUDIO_STAR_PATTERNS: list[str] = [
    r"silent.?switch",
    r"low.?noise.*ldo", r"ultra.?low.?noise.*ldo",
    r"low.?jitter", r"jitter.?attenu", r"jitter.?clean",
    r"\baudio\b",
    r"isolated.*power", r"isolat.*dc.?dc", r"絶縁",
    r"precision.*op.?amp", r"low.?noise.*op.?amp", r"low.?noise.*amplif",
    r"high.?precision.*amp",
    r"audio.*codec", r"audio.*dac", r"audio.*adc",
    r"headphone.*amp", r"ヘッドホン",
    r"class.?d.*amp",
    r"low.?phase.?noise",
    r"femto.?second", r"フェムト",
    r"ocxo", r"tcxo",
]


def extract_opamp_specs(p: dict) -> str:
    """Extract Vos and GBW from op-amp description text. Returns HTML string."""
    genre = classify_genre(p)
    if "オペアンプ" not in genre and "計装アンプ" not in genre:
        return ""
    text = f"{p.get('description', '')} {p.get('category', '')}"
    specs = []
    # GBW: e.g. "5MHz", "10 MHz", "5GHz"
    m = re.search(r"(\d+(?:\.\d+)?)\s*(MHz|GHz|kHz)", text, re.IGNORECASE)
    if m:
        specs.append(f"GBW: {m.group(1)}{m.group(2)}")
    # Vos: e.g. "100µV", "5 µV", "100μV", "ゼロドリフト", "zero-drift"
    m2 = re.search(r"(\d+(?:\.\d+)?)\s*[µμu]V", text)
    if m2:
        specs.append(f"Vos: {m2.group(1)}\u00b5V")
    elif re.search(r"(zero.?drift|ゼロドリフト|チョッパ)", text, re.IGNORECASE):
        specs.append("Vos: \u2248 0 (zero-drift)")
    if not specs:
        return ""
    return '<span class="opamp-specs">' + " | ".join(specs) + "</span>"


def is_audio_star(p: dict) -> bool:
    """True if this product deserves a ★ highlight (especially audio-relevant)."""
    text = f"{p.get('part', '')} {p.get('description', '')} {p.get('category', '')}".lower()
    return any(re.search(pat, text, re.IGNORECASE) for pat in _AUDIO_STAR_PATTERNS)


# ─── HTML Report Generator ───────────────────────────────────────────────────


def _badge_cls(mfr: str) -> str:
    return {"TI": "badge-TI", "ADI": "badge-ADI", "Nisshinbo": "badge-Nisshinbo",
            "Sanken": "badge-Sanken", "Torex": "badge-Torex",
            "Rohm": "badge-Rohm", "Renesas": "badge-Renesas"}.get(mfr, "badge-Other")


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def generate_html(products: list[dict], report_date: str, new_keys: set = None) -> str:
    from collections import defaultdict

    # Translate descriptions EN->JA
    products = translate_descriptions(products)

    # ── Group by ISO week (Mon-Sun), newest first, then by genre ──
    from datetime import date as _date, timedelta as _td

    def _parse_date(ds: str) -> _date | None:
        """Try to parse a date string in various formats. Return None on failure."""
        if not ds:
            return None
        # ISO: "2026-03-23" or "2026-03-23T..."
        try:
            return _date.fromisoformat(ds[:10])
        except (ValueError, TypeError):
            pass
        # "YYYY/MM/DD" or "YYYY/M/D"
        import re as _re
        m = _re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", ds)
        if m:
            return _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        # "Mar. 10, 2026" / "March 2026" etc → use month start
        for fmt in ["%b. %d, %Y", "%B %d, %Y", "%b %d, %Y", "%B %Y", "%b %Y"]:
            try:
                from datetime import datetime as _dt
                return _dt.strptime(ds.strip(), fmt).date()
            except ValueError:
                pass
        return None

    def _week_monday(d: _date) -> _date:
        return d - _td(days=d.weekday())

    _report_date_obj = _date.fromisoformat(report_date[:10])

    def _best_date_obj(p: dict) -> _date | None:
        """announced date if parseable, else found_date. None if neither is useful."""
        ann = _parse_date(p.get("announced") or "")
        if ann:
            return ann
        fd = _parse_date(p.get("found_date") or "")
        if fd:
            return fd
        return None

    def _month_key(d: _date) -> str:
        """Return 'YYYY-MM' string for grouping by month."""
        return d.strftime("%Y-%m")

    def _month_label(ym: str) -> str:
        """'2026-03' → '2026年3月'"""
        y, m = ym.split("-")
        return f"{y}年{int(m)}月"

    genre_order = [g for g, _ in GENRE_RULES] + ["その他"]
    _report_month = _month_key(_report_date_obj)

    # Detect initial bulk-import dates: any found_date with > 50 products
    # (the first few runs pull the entire backlog from manufacturer sites)
    from collections import Counter as _Counter
    _fd_counts = _Counter()
    for p in products:
        fd = _parse_date(p.get("found_date") or "")
        if fd:
            _fd_counts[fd] += 1
    _initial_dates = {d for d, c in _fd_counts.items() if c > 50}

    # Split products into 3 buckets:
    #   1) Weekly: has manufacturer announced date, OR found on a non-bulk day
    #   2) Archive: found on a bulk-import day without manufacturer date
    #   3) Undated: no date at all (rare)
    dated_products = []
    archive_products = []
    undated_products = []
    for p in products:
        ann = _parse_date(p.get("announced") or "")
        fd  = _parse_date(p.get("found_date") or "")
        if ann:
            dated_products.append((p, ann))               # real manufacturer date
        elif fd and fd not in _initial_dates:
            dated_products.append((p, fd))                 # found on a normal day
        elif fd:
            archive_products.append(p)                     # bulk import, no mfr date
        else:
            undated_products.append(p)                     # no date at all

    months_sorted = sorted(
        set(_month_key(bd) for _, bd in dated_products),
        reverse=True,
    ) if dated_products else []
    by_month: dict = {m: defaultdict(list) for m in months_sorted}
    for p, bd in dated_products:
        by_month[_month_key(bd)][classify_genre(p)].append(p)
    # Archive products (initial scan without manufacturer dates)
    archive_by_genre: dict = defaultdict(list)
    for p in archive_products:
        archive_by_genre[classify_genre(p)].append(p)
    # Undated (no date at all)
    undated_by_genre: dict = defaultdict(list)
    for p in undated_products:
        undated_by_genre[classify_genre(p)].append(p)

    total = len(products)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    manufacturers = sorted(set(p["manufacturer"] for p in products))
    mfr_opts = "\n    ".join(
        f'<option value="{m.lower()}">{m}</option>' for m in manufacturers
    )
    categories = sorted(set(
        (p.get("category") or "").split("/")[0].strip()
        for p in products if p.get("category")
    ))
    def _trunc(s: str, n: int = 35) -> str:
        return s[:n] + "…" if len(s) > n else s
    cat_opts = "\n    ".join(
        f'<option value="{_esc(c).lower()}">{_esc(_trunc(c))}</option>' for c in categories
    )

    css = """\
:root { --gold: #C5A55A; --gold-light: #D4B76A; --gold-dim: rgba(197,165,90,.08);
        --bg: #ffffff; --bg-card: #fff; --bg-elev: #f6f6f4; --bg-hover: #f0efe8;
        --text: #1a1a1a; --text-dim: #777; --border: #e0e0e0; }
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue",
               "Hiragino Sans", sans-serif;
  font-size: 15.6px; background: var(--bg); color: var(--text);
}
header {
  background: #1a1a1a; padding: 24px 32px; border-bottom: 1px solid var(--border);
  display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap;
}
header h1 { font-size: 20px; font-weight: 600; letter-spacing: .05em; color: var(--gold); }
header .meta { font-size: 13px; color: #999; }
.filters {
  padding: 14px 32px; background: var(--bg-card); border-bottom: 1px solid var(--border);
  display: flex; flex-wrap: wrap; gap: 12px; align-items: center;
  position: sticky; top: 0; z-index: 100; box-shadow: 0 1px 3px rgba(0,0,0,.04);
}
.filters label { font-size: 12px; color: var(--text-dim); font-weight: 500; text-transform: uppercase; letter-spacing: .06em; }
.filters select, .filters input {
  font-size: 13px; padding: 6px 10px; border: 1px solid var(--border);
  border-radius: 4px; background: var(--bg-card); color: var(--text); cursor: pointer;
  width: auto;
}
.filters select:focus, .filters input:focus { outline: none; border-color: var(--gold); }
.filters input { width: 240px; }
.count-badge {
  margin-left: auto; background: var(--gold); color: #fff;
  padding: 4px 12px; border-radius: 4px; font-size: 12px; font-weight: 700;
}
.date-section { margin: 20px 32px 0; }
.date-section.all-hidden { display: none; }
.date-header {
  display: flex; align-items: center; gap: 10px; cursor: pointer; user-select: none;
  padding: 14px 20px; color: var(--text); border-radius: 6px 6px 0 0;
  font-size: 15px; font-weight: 600; letter-spacing: .02em;
}
.date-header:hover { background: var(--bg-elev); }
.date-header.today-header { background: var(--bg-card); border-left: 3px solid var(--gold); box-shadow: 0 1px 4px rgba(0,0,0,.06); }
.date-header.past-header  { background: var(--bg-elev); }
.date-count {
  font-size: 12px; color: var(--text-dim);
  padding: 2px 10px; border: 1px solid var(--border); border-radius: 4px;
}
.date-today-badge {
  font-size: 10px; background: var(--gold); color: #fff;
  padding: 2px 10px; border-radius: 3px; font-weight: 700; letter-spacing: .06em; text-transform: uppercase;
}
.date-toggle { margin-left: auto; font-size: 11px; color: var(--text-dim); }
.date-section.collapsed .date-body { display: none; }
.date-section.collapsed .date-toggle::before { content: "▶  展開"; }
.date-section:not(.collapsed) .date-toggle::before { content: "▼  折りたたむ"; }
.date-body { border: 1px solid var(--border); border-top: none; border-radius: 0 0 6px 6px; overflow: hidden; }
.genre-section { border-bottom: 1px solid var(--border); }
.genre-section:last-child { border-bottom: none; }
.genre-section.all-hidden { display: none; }
.genre-header {
  display: flex; align-items: center; gap: 8px; cursor: pointer; user-select: none;
  padding: 10px 20px; background: var(--bg-card); color: var(--text-dim);
  font-size: 13px; font-weight: 500;
}
.genre-header:hover { color: var(--text); background: var(--bg-elev); }
.genre-count {
  font-size: 11px; color: var(--text-dim);
  padding: 1px 7px; border: 1px solid var(--border); border-radius: 4px;
}
.genre-toggle { margin-left: auto; font-size: 10px; color: var(--text-dim); }
.genre-section.collapsed .genre-body { display: none; }
.genre-section.collapsed .genre-toggle::before { content: "▶"; }
.genre-section:not(.collapsed) .genre-toggle::before { content: "▼"; }
.genre-body { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; table-layout: fixed; }
thead tr { background: var(--bg-elev); }
th {
  padding: 8px 14px; font-size: 11px; font-weight: 600; text-align: left; color: var(--text-dim);
  letter-spacing: .06em; text-transform: uppercase; white-space: nowrap;
  overflow: hidden; resize: horizontal;
}
td {
  padding: 10px 14px; border-bottom: 1px solid var(--border);
  vertical-align: top; line-height: 1.5; overflow: hidden; text-overflow: ellipsis;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: var(--bg-hover); }
tr.hidden { display: none; }
.mfr-badge {
  display: inline-block; padding: 3px 8px; border-radius: 3px;
  font-size: 10px; font-weight: 600; white-space: nowrap;
  background: var(--bg-elev); color: var(--text-dim); border: 1px solid var(--border);
}
.part-link {
  color: #8B6914; text-decoration: none;
  font-weight: 600; font-size: 13px; font-family: "SF Mono", "Cascadia Code", monospace;
}
.part-link:hover { color: var(--gold); text-decoration: underline; }
.desc-cell { color: var(--text); }
.opamp-specs {
  display: block; margin-top: 4px; font-size: 11px; font-weight: 600;
  color: #2c7be5; font-family: "SF Mono", "Cascadia Code", monospace;
}
.cat-cell  { color: var(--text-dim); font-size: 12px; }
.pkg-cell  { font-family: monospace; font-size: 12px; color: var(--text-dim); }
.date-announced { font-size: 12px; color: var(--text); font-weight: 500; white-space: nowrap; }
.date-found     { font-size: 12px; color: var(--text-dim); font-style: italic; white-space: nowrap; }
.date-label     { font-size: 9px; color: #aaa; text-transform: uppercase; display: block; }
.star-badge { color: var(--gold); font-size: 14px; margin-right: 4px; vertical-align: middle; }
tr.audio-star td { background: var(--gold-dim); }
tr.audio-star:hover td { background: rgba(197,165,90,.14); }
tr.audio-star td:first-child { border-left: 3px solid var(--gold); }
.new-badge {
  display: inline-block; padding: 1px 6px; border-radius: 3px; margin-left: 6px;
  font-size: 9px; font-weight: 700; background: #e74c3c; color: #fff;
  letter-spacing: .06em; vertical-align: middle;
}
.no-products { text-align: center; padding: 60px 20px; color: var(--text-dim); }
.no-products h2 { font-size: 18px; margin-bottom: 8px; color: var(--text); }
@media (max-width: 768px) {
  header, .filters { padding-left: 14px; padding-right: 14px; }
  .date-section { margin-left: 8px; margin-right: 8px; }
}"""

    # ── Helper: build rows for a product list ──────────────────────────────
    def build_rows(prods: list) -> str:
        rows = []
        for p in prods:
            mfr = p["manufacturer"]
            part = _esc(p.get("part") or "")
            desc = _esc(p.get("description") or "")
            cat  = _esc(p.get("category") or "")
            pkg  = _esc(p.get("package") or "")
            url  = p.get("url", "")
            announced_raw = p.get("announced") or ""
            found_raw     = p.get("found_date") or ""
            if announced_raw:
                date_html = f'<span class="date-announced">{_esc(announced_raw)}</span>'
            elif found_raw:
                date_html = (
                    f'<span class="date-found">{_esc(found_raw)}</span>'
                    f'<span class="date-label">\u30af\u30ed\u30fc\u30eb\u767a\u898b\u65e5</span>'
                )
            else:
                date_html = "\u2014"
            star      = is_audio_star(p)
            star_html = '<span class="star-badge">\u2605</span>' if star else ""
            # NEW badge: found_date or announced date == today
            _fd = _parse_date(found_raw)
            _ann = _parse_date(announced_raw)
            is_new = (_fd == _report_date_obj) or (_ann == _report_date_obj)
            if new_keys and p.get("key") in new_keys:
                is_new = True
            new_html  = ' <span class="new-badge">NEW</span>' if is_new else ""
            part_html = (
                f'<a class="part-link" href="{_esc(url)}" target="_blank" rel="noopener">{star_html}{part}{new_html}</a>'
                if url else f"{star_html}{part}{new_html}"
            )
            row_cls = "audio-star" if star else ""
            rows.append(
                f'<tr data-mfr="{mfr.lower()}" data-cat="{_esc(cat).lower()}" class="{row_cls}">'
                f'<td><span class="mfr-badge {_badge_cls(mfr)}">{mfr}</span></td>'
                f'<td>{part_html}</td>'
                f'<td class="desc-cell">{desc}{extract_opamp_specs(p)}</td>'
                f'<td class="cat-cell">{cat}</td>'
                f'<td class="pkg-cell">{pkg}</td>'
                f'<td>{date_html}</td>'
                f'</tr>'
            )
        return "\n".join(rows)

    # ── HTML head ──────────────────────────────────────────────────────────
    parts = [
        f'<!DOCTYPE html>\n<html lang="ja">\n<head>\n'
        f'<meta charset="UTF-8">\n'
        f'<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f'<title>\u30c7\u30d0\u30a4\u30b9\u30cb\u30e5\u30fc\u30b9 {report_date}</title>\n'
        f'<style>\n{css}\n</style>\n</head>\n<body>\n'
        f'<header>\n'
        f'  <h1>\U0001f4e1 \u30c7\u30d0\u30a4\u30b9\u30cb\u30e5\u30fc\u30b9</h1>\n'
        f'  <span class="meta">\u751f\u6210\u65e5\u6642: {now_str}'
        f' &nbsp;|\u200b&nbsp; \u65b0\u88fd\u54c1: <strong>{total}</strong> \u4ef6</span>\n'
        f'</header>\n'
        f'<div class="filters">\n'
        f'  <label>\u30e1\u30fc\u30ab\u30fc</label>\n'
        f'  <select id="filterMfr" onchange="applyFilters()">\n'
        f'    <option value="">\u3059\u3079\u3066</option>\n'
        f'    {mfr_opts}\n'
        f'  </select>\n'
        f'  <label>\u30ab\u30c6\u30b4\u30ea</label>\n'
        f'  <select id="filterCat" onchange="applyFilters()">\n'
        f'    <option value="">\u3059\u3079\u3066</option>\n'
        f'    {cat_opts}\n'
        f'  </select>\n'
        f'  <label>\u691c\u7d22</label>\n'
        f'  <input type="text" id="filterSearch" placeholder="\u54c1\u756a\u30fb\u8aac\u660e\u3092\u691c\u7d22..."\n'
        f'         oninput="applyFilters()" autocomplete="off">\n'
        f'  <span class="count-badge" id="countBadge">{total} \u4ef6</span>\n'
        f'</div>\n'
    ]

    if not products:
        parts.append(
            '<div class="no-products"><h2>\u65b0\u88fd\u54c1\u306f\u3042\u308a\u307e\u305b\u3093</h2>'
            '<p>\u672c\u65e5\u306f\u65b0\u305f\u306a\u88fd\u54c1\u306f\u691c\u51fa\u3055\u308c\u307e\u305b\u3093\u3067\u3057\u305f\u3002</p></div>\n'
        )
    else:
        for ym in months_sorted:
            month_prods = [p for p, bd in dated_products if _month_key(bd) == ym]
            is_current = ym == _report_month
            hdr_cls    = "today-header" if is_current else "past-header"
            sec_cls    = ""             if is_current else " collapsed"
            today_badge = ' <span class="date-today-badge">今月</span>' if is_current else ""
            date_id    = ym.replace("-", "")

            # Genre sub-sections for this month
            genre_parts = []
            for genre in genre_order:
                gprods = by_month[ym].get(genre, [])
                if not gprods:
                    continue
                genre_id  = _esc(genre).replace(" ", "_").replace("/", "_")
                rows_html = build_rows(gprods)
                genre_parts.append(
                    f'<div class="genre-section" id="g_{date_id}_{genre_id}">\n'
                    f'  <div class="genre-header" onclick="toggleGenre(this)">\n'
                    f'    <span>{_esc(genre)}</span>\n'
                    f'    <span class="genre-count">{len(gprods)} 件</span>\n'
                    f'    <span class="genre-toggle"></span>\n'
                    f'  </div>\n'
                    f'  <div class="genre-body">\n'
                    f'    <table>\n'
                    f'      <thead><tr>\n'
                    f'        <th>メーカー</th><th>品番</th>'
                    f'<th>説明 (JA)</th>\n'
                    f'        <th>カテゴリ</th>'
                    f'<th>パッケージ</th><th>発表日</th>\n'
                    f'      </tr></thead>\n'
                    f'      <tbody>\n{rows_html}\n      </tbody>\n'
                    f'    </table>\n'
                    f'  </div>\n'
                    f'</div>\n'
                )
            genre_html = "\n".join(genre_parts)
            parts.append(
                f'<div class="date-section{sec_cls}" id="date_{date_id}">\n'
                f'  <div class="date-header {hdr_cls}" onclick="toggleDate(this)">\n'
                f'    <span>{_month_label(ym)}</span>{today_badge}\n'
                f'    <span class="date-count">{len(month_prods)} 件</span>\n'
                f'    <span class="date-toggle"></span>\n'
                f'  </div>\n'
                f'  <div class="date-body">\n{genre_html}\n  </div>\n'
                f'</div>\n'
            )

        # ── Archive section (initial scan products without manufacturer dates) ──
        if archive_products:
            genre_parts = []
            for genre in genre_order:
                gprods = archive_by_genre.get(genre, [])
                if not gprods:
                    continue
                genre_id = _esc(genre).replace(" ", "_").replace("/", "_")
                rows_html = build_rows(gprods)
                genre_parts.append(
                    f'<div class="genre-section" id="g_archive_{genre_id}">\n'
                    f'  <div class="genre-header" onclick="toggleGenre(this)">\n'
                    f'    <span>{_esc(genre)}</span>\n'
                    f'    <span class="genre-count">{len(gprods)} 件</span>\n'
                    f'    <span class="genre-toggle"></span>\n'
                    f'  </div>\n'
                    f'  <div class="genre-body">\n'
                    f'    <table>\n'
                    f'      <thead><tr>\n'
                    f'        <th>メーカー</th><th>品番</th>'
                    f'<th>説明 (JA)</th>\n'
                    f'        <th>カテゴリ</th>'
                    f'<th>パッケージ</th><th>発表日</th>\n'
                    f'      </tr></thead>\n'
                    f'      <tbody>\n{rows_html}\n      </tbody>\n'
                    f'    </table>\n'
                    f'  </div>\n'
                    f'</div>\n'
                )
            genre_html = "\n".join(genre_parts)
            _init_labels = ", ".join(d.isoformat() for d in sorted(_initial_dates))
            parts.append(
                f'<div class="date-section collapsed" id="date_archive">\n'
                f'  <div class="date-header past-header" onclick="toggleDate(this)">\n'
                f'    <span>アーカイブ（初回取込）</span>\n'
                f'    <span class="date-count">{len(archive_products)} 件</span>\n'
                f'    <span class="date-toggle"></span>\n'
                f'  </div>\n'
                f'  <div class="date-body">\n{genre_html}\n  </div>\n'
                f'</div>\n'
            )

        # ── Undated section (products with no date at all — should be rare) ──
        if undated_products:
            genre_parts = []
            for genre in genre_order:
                gprods = undated_by_genre.get(genre, [])
                if not gprods:
                    continue
                genre_id = _esc(genre).replace(" ", "_").replace("/", "_")
                rows_html = build_rows(gprods)
                genre_parts.append(
                    f'<div class="genre-section" id="g_undated_{genre_id}">\n'
                    f'  <div class="genre-header" onclick="toggleGenre(this)">\n'
                    f'    <span>{_esc(genre)}</span>\n'
                    f'    <span class="genre-count">{len(gprods)} 件</span>\n'
                    f'    <span class="genre-toggle"></span>\n'
                    f'  </div>\n'
                    f'  <div class="genre-body">\n'
                    f'    <table>\n'
                    f'      <thead><tr>\n'
                    f'        <th>メーカー</th><th>品番</th>'
                    f'<th>説明 (JA)</th>\n'
                    f'        <th>カテゴリ</th>'
                    f'<th>パッケージ</th><th>発表日</th>\n'
                    f'      </tr></thead>\n'
                    f'      <tbody>\n{rows_html}\n      </tbody>\n'
                    f'    </table>\n'
                    f'  </div>\n'
                    f'</div>\n'
                )
            genre_html = "\n".join(genre_parts)
            parts.append(
                f'<div class="date-section collapsed" id="date_undated">\n'
                f'  <div class="date-header past-header" onclick="toggleDate(this)">\n'
                f'    <span>日付不明</span>\n'
                f'    <span class="date-count">{len(undated_products)} 件</span>\n'
                f'    <span class="date-toggle"></span>\n'
                f'  </div>\n'
                f'  <div class="date-body">\n{genre_html}\n  </div>\n'
                f'</div>\n'
            )

    # ── Footer / JS ────────────────────────────────────────────────────────
    parts.append(
        '<div style="height:40px"></div>\n'
        '<script>\n'
        'function toggleDate(el) {\n'
        '  el.closest(".date-section").classList.toggle("collapsed");\n'
        '}\n'
        'function toggleGenre(el) {\n'
        '  el.closest(".genre-section").classList.toggle("collapsed");\n'
        '}\n'
        'function applyFilters() {\n'
        '  const mfr = document.getElementById("filterMfr").value.toLowerCase();\n'
        '  const cat = document.getElementById("filterCat").value.toLowerCase();\n'
        '  const q   = document.getElementById("filterSearch").value.toLowerCase();\n'
        '  const rows = document.querySelectorAll("tbody tr");\n'
        '  let vis = 0;\n'
        '  rows.forEach(row => {\n'
        '    const mfrOk = !mfr || row.dataset.mfr === mfr;\n'
        '    const catOk = !cat || (row.dataset.cat && row.dataset.cat.includes(cat));\n'
        '    const qOk   = !q || row.innerText.toLowerCase().includes(q);\n'
        '    const show  = mfrOk && catOk && qOk;\n'
        '    row.classList.toggle("hidden", !show);\n'
        '    if (show) vis++;\n'
        '  });\n'
        '  // Show/hide genre and date sections based on visible rows\n'
        '  document.querySelectorAll(".genre-section").forEach(sec => {\n'
        '    const any = sec.querySelectorAll("tbody tr:not(.hidden)").length > 0;\n'
        '    sec.classList.toggle("all-hidden", !any);\n'
        '    if (any && (mfr || q)) sec.classList.remove("collapsed");\n'
        '  });\n'
        '  document.querySelectorAll(".date-section").forEach(sec => {\n'
        '    const any = sec.querySelectorAll("tbody tr:not(.hidden)").length > 0;\n'
        '    sec.classList.toggle("all-hidden", !any);\n'
        '    if (any && (mfr || q)) sec.classList.remove("collapsed");\n'
        '  });\n'
        '  document.getElementById("countBadge").textContent = vis + " \u4ef6";\n'
        '}\n'
        '// Column width persistence\n'
        '(function(){\n'
        '  const KEY = "dn-col-widths";\n'
        '  function saveWidths(){\n'
        '    const ths = document.querySelectorAll("thead tr:first-child th");\n'
        '    if(!ths.length) return;\n'
        '    const first = document.querySelector("thead tr");\n'
        '    if(!first) return;\n'
        '    const ws={}; first.querySelectorAll("th").forEach((th,i)=>{ws[i]=th.offsetWidth});\n'
        '    localStorage.setItem(KEY, JSON.stringify(ws));\n'
        '  }\n'
        '  function restoreWidths(){\n'
        '    const s = localStorage.getItem(KEY);\n'
        '    if(!s) return;\n'
        '    try{\n'
        '      const ws = JSON.parse(s);\n'
        '      document.querySelectorAll("thead tr").forEach(row=>{\n'
        '        row.querySelectorAll("th").forEach((th,i)=>{\n'
        '          if(ws[i]) th.style.width = ws[i]+"px";\n'
        '        });\n'
        '      });\n'
        '    }catch(e){}\n'
        '  }\n'
        '  restoreWidths();\n'
        '  const obs = new ResizeObserver(()=>{ clearTimeout(obs._t); obs._t=setTimeout(saveWidths,500); });\n'
        '  document.querySelectorAll("thead th").forEach(th => obs.observe(th));\n'
        '})();\n'
        '</script>\n'
        '</body>\n</html>\n'
    )

    return "\n".join(parts)


# ─── Password protection (XOR encryption) ────────────────────────────────────

REPORT_PASSWORD = "brise2026"   # ← チーム共有パスワード（変更可）


def protect_html(raw_html: str) -> str:
    """Encrypt HTML with password so the page requires login to view.

    Uses Blob URL redirect instead of document.write() to avoid CSS leakage
    from the gate page into the decrypted report.
    """
    import hashlib
    import base64

    key = hashlib.sha256(REPORT_PASSWORD.encode()).digest()
    body_bytes = raw_html.encode("utf-8")
    encrypted = bytes(b ^ key[i % len(key)] for i, b in enumerate(body_bytes))
    encoded = base64.b64encode(encrypted).decode("ascii")

    return (
        '<!DOCTYPE html>\n<html lang="ja"><head><meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        '<title>デバイスニュース</title>\n'
        '<style>\n'
        '*{box-sizing:border-box;margin:0;padding:0}\n'
        'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Hiragino Sans",sans-serif;'
        'display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5}\n'
        '.gate{background:#fff;padding:48px;border-radius:10px;box-shadow:0 2px 16px rgba(0,0,0,.08);'
        'text-align:center;max-width:360px;width:90%}\n'
        '.gate h2{font-size:20px;color:#1a1a1a;margin-bottom:6px}\n'
        '.gate p{font-size:13px;color:#888;margin-bottom:20px}\n'
        '.gate input{width:100%;padding:12px;font-size:15px;border:1px solid #ddd;'
        'border-radius:6px;margin-bottom:14px;box-sizing:border-box}\n'
        '.gate input:focus{outline:none;border-color:#C5A55A}\n'
        '.gate button{width:100%;padding:12px;background:#C5A55A;color:#fff;border:none;'
        'border-radius:6px;font-size:15px;font-weight:600;cursor:pointer;letter-spacing:.04em}\n'
        '.gate button:hover{background:#b8953e}\n'
        '.gate .err{color:#e74c3c;font-size:13px;margin-top:8px;display:none}\n'
        '</style></head><body>\n'
        '<div class="gate" id="gate">\n'
        '  <h2>📡 デバイスニュース</h2>\n'
        '  <p>パスワードを入力してください</p>\n'
        '  <input type="password" id="pw" placeholder="パスワード" '
        'onkeydown="if(event.key===\'Enter\')doLogin()" autofocus>\n'
        '  <button onclick="doLogin()">ログイン</button>\n'
        '  <p class="err" id="err">パスワードが違います</p>\n'
        '</div>\n'
        '<script>\n'
        f'const D="{encoded}";\n'
        'async function sha256(s){const b=await crypto.subtle.digest("SHA-256",'
        'new TextEncoder().encode(s));return new Uint8Array(b)}\n'
        'async function dec(pw){const k=await sha256(pw);const r=atob(D);'
        'const b=new Uint8Array(r.length);for(let i=0;i<r.length;i++)'
        'b[i]=r.charCodeAt(i)^k[i%k.length];return new TextDecoder().decode(b)}\n'
        'function showReport(html){\n'
        '  var doc=new DOMParser().parseFromString(html,"text/html");\n'
        '  document.head.innerHTML=doc.head.innerHTML;\n'
        '  document.body.innerHTML=doc.body.innerHTML;\n'
        '  document.body.removeAttribute("style");\n'
        '  document.body.className="";\n'
        '  document.body.querySelectorAll("script").forEach(function(old){\n'
        '    var s=document.createElement("script");s.textContent=old.textContent;\n'
        '    old.parentNode.replaceChild(s,old);\n'
        '  });\n'
        '}\n'
        'async function doLogin(){const pw=document.getElementById("pw").value;\n'
        '  try{const h=await dec(pw);\n'
        '    if(h.includes("<!DOCTYPE")||h.includes("<html")){\n'
        '      localStorage.setItem("dn-pw",pw);showReport(h);}\n'
        '    else{document.getElementById("err").style.display="block"}\n'
        '  }catch(e){document.getElementById("err").style.display="block"}}\n'
        '(async()=>{const s=localStorage.getItem("dn-pw");if(s){try{const h=await dec(s);\n'
        '  if(h.includes("<!DOCTYPE")||h.includes("<html")){showReport(h)}\n'
        '}catch(e){localStorage.removeItem("dn-pw")}}})();\n'
        '</script></body></html>\n'
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def _git_backup(today: str, new_count: int):
    """Commit DB + reports to git and push."""
    import subprocess
    git = BASE_DIR / ".git"
    if not git.exists():
        return
    try:
        subprocess.run(["git", "add", str(DB_PATH), str(TRANS_CACHE_PATH),
                        str(REPORTS_DIR)],
                       cwd=BASE_DIR, check=True, capture_output=True)
        msg = f"scraper: {today} ({new_count} new products)"
        result = subprocess.run(
            ["git", "commit", "-m", msg],
            cwd=BASE_DIR, capture_output=True, text=True
        )
        if result.returncode == 0:
            log.info(f"Git commit: {msg}")
            push = subprocess.run(["git", "push"], cwd=BASE_DIR,
                                  capture_output=True, text=True)
            if push.returncode == 0:
                log.info("Git push: OK")
            else:
                log.warning(f"Git push failed: {push.stderr.strip()}")
        elif "nothing to commit" in (result.stdout + result.stderr):
            log.debug("Git: nothing new to commit")
        else:
            log.warning(f"Git commit failed: {result.stderr.strip()}")
    except Exception as e:
        log.warning(f"Git backup skipped: {e}")


def run():
    today = date.today().isoformat()
    log.info(f"=== Device News Scraper starting: {today} ===")

    db = load_db()

    all_scraped: list[dict] = []
    for scraper_fn in [scrape_ti, scrape_adi, scrape_nisshinbo,
                       scrape_sanken, scrape_torex, scrape_rohm,
                       scrape_renesas]:
        try:
            prods = scraper_fn()
            all_scraped.extend(prods)
        except Exception as e:
            log.error(f"{scraper_fn.__name__} failed: {e}", exc_info=True)

    log.info(f"Total audio-relevant products scraped: {len(all_scraped)}")

    if not all_scraped:
        log.error("No products scraped at all — network issue? Skipping DB/report update.")
        return []

    new_keys = update_db(all_scraped, db)
    log.info(f"New products (not seen before): {len(new_keys)}")

    db["last_run"] = datetime.now().isoformat()
    save_db(db)
    log.info(f"DB saved. Backup: backups/known_products_{today}.json")

    # Generate report from ALL known products in DB
    all_products = list(db.get("products", {}).values())
    log.info(f"Total products in DB for report: {len(all_products)}")

    html = generate_html(all_products, today, new_keys)
    protected = protect_html(html)

    report_path = REPORTS_DIR / f"{today}.html"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(protected)
    log.info(f"Report saved: {report_path}")

    # index.html for GitHub Pages
    index_path = REPORTS_DIR / "index.html"
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(protected)

    _git_backup(today, len(new_keys))

    if new_keys:
        new_prods = [p for p in all_scraped if p["key"] in new_keys]
        log.info(f"New products summary ({len(new_prods)}):")
        for p in new_prods[:20]:
            log.info(f"  [{p['manufacturer']}] {p['part']} - {p['description'][:70]}")
        if len(new_prods) > 20:
            log.info(f"  ... and {len(new_prods) - 20} more")
    else:
        log.info("No new products found today.")

    return [p for p in all_scraped if p["key"] in new_keys]


if __name__ == "__main__":
    run()
