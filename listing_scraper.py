from __future__ import annotations

import os
import random
import re
import threading
import time
from typing import Callable, Optional

from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By

from scraper import ScraperConfig, UC_DRIVER_CREATE_LOCK, detect_installed_chrome_major

DETAIL_FIELDS = [
    "Source URL",
    "Title",
    "Price",
    "Address",
    "Property Type",
    "Size (sqft)",
    "PSF",
    "Description",
    "Tenure",
    "Agent Name",
    "Agent Company",
    "Phone",
    "Email",
]


# ── File reading helpers ──────────────────────────────────────────────────────

def read_urls_from_file(file_path: str, url_column: str) -> list[str]:
    """Read listing URLs from an Excel (.xlsx) or CSV (.csv) file.

    Looks for *url_column* (case-insensitive) and returns every cell that
    starts with ``http``.
    """
    ext = os.path.splitext(file_path)[1].lower()
    urls: list[str] = []

    if ext == ".xlsx":
        wb = load_workbook(file_path, read_only=True, data_only=True)
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        first_row = next(rows_iter, None)
        if first_row is None:
            wb.close()
            return []
        headers = [str(h) if h is not None else "" for h in first_row]
        col_idx = _find_col_index(headers, url_column)
        if col_idx < 0:
            wb.close()
            raise ValueError(
                f"Column '{url_column}' not found in file.\n"
                f"Available columns: {headers}"
            )
        for row in rows_iter:
            if col_idx < len(row):
                val = row[col_idx]
                if val and str(val).strip().lower().startswith("http"):
                    urls.append(str(val).strip())
        wb.close()

    elif ext == ".csv":
        with open(file_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            field_names: list[str] = list(reader.fieldnames or [])
            actual_col = _find_col_name(field_names, url_column)
            if actual_col is None:
                raise ValueError(
                    f"Column '{url_column}' not found in CSV.\n"
                    f"Available columns: {field_names}"
                )
            for row in reader:
                val = row.get(actual_col, "").strip()
                if val.lower().startswith("http"):
                    urls.append(val)
    else:
        raise ValueError(
            f"Unsupported file type '{ext}'. Please use .xlsx or .csv."
        )

    return urls


def _find_col_index(headers: list[str], target: str) -> int:
    t = target.strip().lower()
    for i, h in enumerate(headers):
        if h.strip().lower() == t:
            return i
    return -1


def _find_col_name(names: list[str], target: str) -> Optional[str]:
    t = target.strip().lower()
    for n in names:
        if n.strip().lower() == t:
            return n
    return None


# ── Scraper ───────────────────────────────────────────────────────────────────

class DirectListingScraper:
    """Scrapes individual listing detail pages from a given list of direct URLs.

    Each URL is opened in Chrome, the page HTML is parsed, and the extracted
    data is written as a single row in an output Excel file.  One Excel file is
    produced per run (all listings merged into one sheet).
    """

    # Classes that indicate a listing detail page has loaded
    _WAIT_CLASSES = [
        "amount",
        "price-amount",
        "listing-specs",
        "listing-amenities",
        "amenities",
        "property-address",
        "agent-info",
        "listed-by",
    ]

    def __init__(
        self,
        urls: list[str],
        config: ScraperConfig,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[dict], None]] = None,
        stop_requested: Optional[Callable[[], bool]] = None,
    ):
        self.urls = urls
        self.config = config
        self.log = log_callback or (lambda _: None)
        self.progress = progress_callback or (lambda _: None)
        self.stop_requested = stop_requested or (lambda: False)
        self.error_count = 0
        self.start_time = 0.0
        self._driver: uc.Chrome | None = None
        self._force_stop = False
        self._driver_lock = threading.Lock()

    # ── Stop handling ─────────────────────────────────────────────────────────

    def request_stop(self, force: bool = False) -> None:
        self._force_stop = True
        if not force:
            return
        with self._driver_lock:
            driver = self._driver
        if driver is None:
            return
        try:
            if self._is_driver_alive(driver):
                driver.quit()
        except Exception:
            pass
        finally:
            with self._driver_lock:
                if self._driver is driver:
                    self._driver = None

    def _should_stop(self) -> bool:
        return self._force_stop or self.stop_requested()

    def _sleep_with_stop(self, seconds: float) -> None:
        deadline = time.time() + seconds
        while time.time() < deadline:
            if self._should_stop():
                raise RuntimeError("Stop requested. Aborting scrape.")
            time.sleep(0.1)

    def _polite_wait(self, min_s: float = 2.0, max_s: float = 4.0) -> None:
        self._sleep_with_stop(random.uniform(min_s, max_s))

    # ── Driver management ─────────────────────────────────────────────────────

    def _await_first_window(self, driver: uc.Chrome, timeout: float = 5.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._should_stop():
                raise RuntimeError("Stop requested.")
            try:
                if driver.window_handles:
                    return True
            except Exception:
                pass
            time.sleep(0.1)
        return False

    def _is_driver_alive(self, driver: uc.Chrome) -> bool:
        try:
            _ = driver.window_handles
            return True
        except Exception:
            return False

    def _create_driver(self) -> uc.Chrome:
        attempts = self.config.retries + 1
        last_error: Exception | None = None
        preferred_major = self.config.chrome_major or detect_installed_chrome_major()

        for attempt in range(1, attempts + 1):
            if self._should_stop():
                raise RuntimeError("Stop requested before browser init.")
            self.log(f"Starting browser ({attempt}/{attempts})...")
            driver: uc.Chrome | None = None
            try:
                options = uc.ChromeOptions()
                options.page_load_strategy = "eager"
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-blink-features=AutomationControlled")
                if self.config.headless:
                    options.add_argument("--headless=new")

                with UC_DRIVER_CREATE_LOCK:
                    if preferred_major is not None:
                        self.log(f"Using Chrome major {preferred_major}.")
                        driver = uc.Chrome(
                            options=options,
                            use_subprocess=True,
                            version_main=preferred_major,
                        )
                    else:
                        driver = uc.Chrome(options=options, use_subprocess=True)

                if not self._await_first_window(driver):
                    raise RuntimeError("No active Chrome window became available.")
                driver.set_window_size(1366, 900)
                with self._driver_lock:
                    self._driver = driver
                return driver

            except Exception as exc:
                last_error = exc
                mismatch = re.search(r"Current browser version is\s+(\d+)\.", str(exc))
                if mismatch:
                    preferred_major = int(mismatch.group(1))
                    self.log(f"Re-detected Chrome major {preferred_major} from error; retrying.")
                if attempt < attempts:
                    self.log(f"Browser launch failed: {exc}. Retrying...")
                    try:
                        if driver:
                            driver.quit()
                    except Exception:
                        pass
                    self._sleep_with_stop(1.5)

        raise RuntimeError(
            f"Failed to init browser after {attempts} attempts: {last_error}"
        )

    def _wait_for_any_class(
        self, driver: uc.Chrome, class_names: list[str], timeout: int
    ) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._should_stop():
                raise RuntimeError("Stop requested.")
            if not self._is_driver_alive(driver):
                raise RuntimeError("Browser was closed.")
            for cls in class_names:
                try:
                    if driver.find_elements(By.CLASS_NAME, cls):
                        return True
                except Exception:
                    pass
            time.sleep(0.2)
        return False

    def _navigate_to_url(self, driver: uc.Chrome, url: str) -> bool:
        attempts = self.config.retries + 1
        for attempt in range(1, attempts + 1):
            if self._should_stop():
                raise RuntimeError("Stop requested.")
            if not self._is_driver_alive(driver):
                raise RuntimeError("Browser was closed.")
            try:
                driver.set_page_load_timeout(self.config.timeout_sec)
                driver.get(url)
            except TimeoutException:
                pass
            except WebDriverException:
                if self._should_stop():
                    raise RuntimeError("Stop requested.")
                raise RuntimeError("Browser closed during navigation.")
            except Exception:
                if attempt == attempts:
                    return False
                continue

            if self._wait_for_any_class(driver, self._WAIT_CLASSES, self.config.timeout_sec):
                self._polite_wait()
                return True

            if attempt < attempts:
                self.log(f"Retrying load ({attempt}/{attempts - 1}): {url}")

        return False

    # ── HTML parsing ──────────────────────────────────────────────────────────

    @staticmethod
    def _find_text(soup: BeautifulSoup, selectors: list[str]) -> Optional[str]:
        for sel in selectors:
            try:
                elem = soup.select_one(sel)
                if elem:
                    text = elem.get_text(separator=" ", strip=True)
                    if text:
                        return text
            except Exception:
                continue
        return None

    @staticmethod
    def _extract_meta_table(soup: BeautifulSoup) -> dict[str, str]:
        """Return a dict of label → value from the listing spec/meta table."""
        result: dict[str, str] = {}
        # Cover both PropertyGuru and CommercialGuru meta table structures
        for item in soup.select(
            ".listing-specifications__item, "
            ".meta-table__item, "
            ".listing-amenities__item"
        ):
            label_elem = item.select_one(
                ".listing-specifications__label, "
                ".meta-table__item__label, "
                ".meta-table__item__wrapper__label, "
                ".amenity-label"
            )
            value_elem = item.select_one(
                ".listing-specifications__value, "
                ".meta-table__item__value, "
                ".meta-table__item__wrapper__value, "
                ".amenity-value"
            )
            if label_elem and value_elem:
                k = label_elem.get_text(strip=True)
                v = value_elem.get_text(strip=True)
                if k and v:
                    result[k] = v
        return result

    @staticmethod
    def _extract_size_sqft(soup: BeautifulSoup) -> Optional[str]:
        for da_id in ("area-amenity", "floor-area-amenity", "land-area-amenity"):
            wrapper = soup.find("div", attrs={"da-id": da_id})
            if wrapper:
                for p in wrapper.find_all("p"):
                    text = p.get_text(strip=True)
                    if "sqft" in text.lower():
                        return text
        # Regex fallback anywhere on the page
        m = re.search(r"([\d,]+\s*sqft)", soup.get_text(separator=" "), re.IGNORECASE)
        return m.group(1).strip() if m else None

    @staticmethod
    def _extract_psf(soup: BeautifulSoup) -> Optional[str]:
        wrapper = soup.find("div", attrs={"da-id": "psf-amenity"})
        if wrapper:
            for p in wrapper.find_all("p"):
                text = p.get_text(strip=True)
                if "S$" in text:
                    return text
        m = re.search(
            r"S\$[\d,]+(?:\.\d+)?\s*(?:psf|/ sqft)",
            soup.get_text(separator=" "),
            re.IGNORECASE,
        )
        return m.group(0).strip() if m else None

    @staticmethod
    def _extract_phone(soup: BeautifulSoup) -> Optional[str]:
        for a in soup.find_all("a", href=re.compile(r"^tel:")):
            num = a["href"].replace("tel:", "").strip()
            if num:
                return num
        for sel in (
            '[da-id="agent-phone"]',
            ".agent-phone",
            ".phone-number",
            ".contact-phone",
        ):
            elem = soup.select_one(sel)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return text
        return None

    @staticmethod
    def _extract_email(soup: BeautifulSoup) -> Optional[str]:
        for a in soup.find_all("a", href=re.compile(r"^mailto:")):
            addr = a["href"].replace("mailto:", "").strip()
            if addr:
                return addr
        for sel in (
            '[da-id="agent-email"]',
            ".agent-email",
            ".contact-email",
        ):
            elem = soup.select_one(sel)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return text
        return None

    def _parse_listing_page(self, html: str, url: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")

        title = self._find_text(soup, [
            'h1[data-automation-id="listing-title"]',
            '[da-id="listing-title"]',
            "h1.listing__title",
            ".listing-name",
            "h1",
        ])

        price = self._find_text(soup, [
            "h2.amount",
            '[da-id="price-amount"]',
            ".amount",
            ".listing-price",
        ])

        address = self._find_text(soup, [
            '[da-id="property-address"]',
            ".property-address",
            ".listing-address",
            ".address",
        ])

        meta = self._extract_meta_table(soup)
        meta_lower = {k.lower(): v for k, v in meta.items()}

        property_type = (
            meta_lower.get("property type")
            or meta_lower.get("type")
            or self._find_text(soup, [
                ".listing__property-type",
                '[da-id="property-type"]',
            ])
        )

        tenure = (
            meta_lower.get("tenure")
            or self._find_text(soup, [
                '[da-id="tenure-value"]',
                ".meta-table__item__wrapper__value",
            ])
        )

        size_sqft = self._extract_size_sqft(soup)
        psf = self._extract_psf(soup)

        description = self._find_text(soup, [
            '[da-id="listing-description"]',
            ".listing-description",
            ".description",
            ".listing__description",
        ])
        # Cap very long descriptions so the Excel cell stays manageable
        if description and len(description) > 2000:
            description = description[:1997] + "..."

        agent_name = self._find_text(soup, [
            ".agent-name",
            ".agent-info__name",
            ".listed-by__agent-name",
            '[da-id="agent-name"]',
        ])

        agent_company = self._find_text(soup, [
            ".agent-company-name",
            ".agency-name",
            ".agent-info__company",
            '[da-id="agent-company"]',
            ".agent-agency",
        ])

        phone = self._extract_phone(soup)
        email = self._extract_email(soup)

        return {
            "Source URL": url,
            "Title": title,
            "Price": price,
            "Address": address,
            "Property Type": property_type,
            "Size (sqft)": size_sqft,
            "PSF": psf,
            "Description": description,
            "Tenure": tenure,
            "Agent Name": agent_name,
            "Agent Company": agent_company,
            "Phone": phone,
            "Email": email,
        }

    # ── Main run loop ─────────────────────────────────────────────────────────

    def run(self) -> dict:
        self.start_time = time.time()
        total = len(self.urls)
        self.log(f"Direct listing scrape — {total} URL(s) to process")

        driver = self._create_driver()

        # Combined Excel — one row per listing
        wb = Workbook()
        ws = wb.active
        ws.title = "Listings"
        ws.append(DETAIL_FIELDS)

        processed = 0
        try:
            for i, url in enumerate(self.urls, start=1):
                if self._should_stop():
                    self.log("Stop requested. Finishing current listing.")
                    break

                self.log(f"[{i}/{total}] Loading: {url}")
                loaded = self._navigate_to_url(driver, url)

                if not loaded:
                    self.error_count += 1
                    row_data: dict = {f: None for f in DETAIL_FIELDS}
                    row_data["Source URL"] = url
                    self.log(f"[{i}/{total}] FAILED to load: {url}")
                else:
                    row_data = self._parse_listing_page(driver.page_source, url)
                    label = row_data.get("Title") or row_data.get("Price") or url
                    self.log(f"[{i}/{total}] OK — {label}")

                ws.append([row_data.get(field) for field in DETAIL_FIELDS])
                processed += 1

                self.progress({
                    "stage": "listings",
                    "current_page": i,
                    "total_pages": total,
                    "listings_processed": processed,
                    "total_listings": total,
                    "error_count": self.error_count,
                    "elapsed": int(time.time() - self.start_time),
                })

            wb.save(self.config.output_csv)
            self.log(f"Saved Excel: {self.config.output_csv}")

        finally:
            try:
                if self._is_driver_alive(driver):
                    driver.quit()
            except Exception:
                pass
            with self._driver_lock:
                self._driver = None

        return {
            "total_pages": total,
            "total_links": total,
            "processed": processed,
            "errors": self.error_count,
            "elapsed": int(time.time() - self.start_time),
        }
