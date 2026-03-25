from __future__ import annotations

import random
import re
import threading
import time
from typing import Callable, Optional
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from openpyxl import Workbook  # type: ignore[reportMissingModuleSource]
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By

from contact_phone_extractor import reveal_and_extract_agent_phone
from login_manager import login_commercialguru

from scraper import (
    ScraperConfig,
    STANDARD_CSV_FIELDS,
    UC_DRIVER_CREATE_LOCK,
    extract_baths_from_soup,
    extract_beds_from_soup,
    extract_land_size_from_soup,
    detect_installed_chrome_major,
    extract_psf_from_soup,
)

COMMERCIAL_RESULTS_URL = "https://www.commercialguru.com.sg/property-for-sale"


class CommercialGuruScraper:
    def __init__(
        self,
        config: ScraperConfig,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[dict], None]] = None,
        stop_requested: Optional[Callable[[], bool]] = None,
    ):
        self.config = config
        self.log = log_callback or (lambda _: None)
        self.progress = progress_callback or (lambda _: None)
        self.stop_requested = stop_requested or (lambda: False)
        self.error_count = 0
        self.start_time = 0.0
        self._driver: uc.Chrome | None = None
        self._force_stop = False
        self._driver_lock = threading.Lock()

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

    @staticmethod
    def _format_launch_error(exc: Exception) -> str:
        text = str(exc).strip()
        if "Stacktrace:" in text:
            text = text.split("Stacktrace:", 1)[0].strip()
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return exc.__class__.__name__
        return " | ".join(lines[:2])

    def _polite_wait(self, min_seconds: float = 2.0, max_seconds: float = 4.0, jitter: float = 0.35) -> None:
        base_wait = random.uniform(min_seconds, max_seconds)
        jittered_wait = max(0.2, base_wait + random.uniform(-jitter, jitter))
        self._sleep_with_stop(jittered_wait)

    def _await_first_window(self, driver: uc.Chrome, timeout: float = 5.0) -> bool:
        deadline = time.time() + timeout
        recovery_attempted = False
        while time.time() < deadline:
            if self._should_stop():
                raise RuntimeError("Stop requested. Aborting scrape.")
            try:
                handles = driver.window_handles
                if handles:
                    try:
                        driver.switch_to.window(handles[0])
                    except Exception:
                        pass
                    return True
            except Exception:
                pass

            if not recovery_attempted:
                recovery_attempted = True
                try:
                    driver.switch_to.new_window("tab")
                    continue
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

    def _get_query_districts(self) -> str:
        codes = [x.strip() for x in self.config.district_codes if x.strip()]
        for x in self.config.extra_params.get("districtCode", []):
            clean = x.strip()
            if clean:
                codes.append(clean)
        if not codes:
            codes = re.findall(r"\bD\d{2}\b", self.config.freetext_display.upper())
        unique = list(dict.fromkeys(codes))
        return ", ".join(unique)

    def _build_query_string(self) -> str:
        params: list[tuple[str, str]] = []
        params.append(("_freetextDisplay", self.config.freetext_display))
        for code in self.config.district_codes:
            params.append(("districtCode", code))
        for bed in self.config.bedrooms:
            params.append(("bedrooms", bed))

        params.append(("minPrice", str(self.config.min_price)))
        params.append(("maxPrice", str(self.config.max_price)))

        if self.config.property_type:
            params.append(("propertyTypeGroup", self.config.property_type))
        if self.config.tenure:
            params.append(("tenure", self.config.tenure))
        if self.config.furnishing:
            params.append(("furnishing", self.config.furnishing))

        for key, values in self.config.extra_params.items():
            for value in values:
                params.append((key, value))

        return urlencode(params, doseq=True)

    def _build_page_url(self, page: int) -> str:
        base = self.config.base_url or COMMERCIAL_RESULTS_URL
        return f"{base}?{self._build_query_string()}&page={page}"

    def _create_driver(self) -> uc.Chrome:
        attempts = self.config.retries + 1
        last_error: Exception | None = None
        preferred_major = self.config.chrome_major or detect_installed_chrome_major()

        for attempt in range(1, attempts + 1):
            if self._should_stop():
                raise RuntimeError("Stop requested before browser initialization.")

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
                        self.log(f"Using Chrome major {preferred_major} for driver compatibility.")
                        driver = uc.Chrome(
                            options=options,
                            use_subprocess=True,
                            version_main=preferred_major,
                        )
                    else:
                        driver = uc.Chrome(options=options, use_subprocess=True)

                if not self._await_first_window(driver):
                    raise RuntimeError("Chrome started but no active window became available")

                driver.set_window_size(1366, 900)
                with self._driver_lock:
                    self._driver = driver
                return driver
            except Exception as exc:
                last_error = exc
                mismatch = re.search(r"Current browser version is\s+(\d+)\.", str(exc))
                if mismatch:
                    preferred_major = int(mismatch.group(1))
                    self.log(
                        f"Detected local Chrome major {preferred_major} from launch error; retrying with that version."
                    )
                if attempt < attempts:
                    self.log(f"Browser launch failed: {self._format_launch_error(exc)}. Retrying...")
                    try:
                        if driver is not None:
                            driver.quit()
                    except Exception:
                        pass
                    self._sleep_with_stop(1.5)

        raise RuntimeError(f"Failed to initialize browser after {attempts} attempts: {last_error}")

    def _wait_for_any_class(self, driver: uc.Chrome, class_names: list[str], timeout: int) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._should_stop():
                raise RuntimeError("Stop requested. Aborting scrape.")
            if not self._is_driver_alive(driver):
                if self._should_stop():
                    raise RuntimeError("Stop requested. Aborting scrape.")
                raise RuntimeError("Browser was closed. Aborting scrape.")

            for class_name in class_names:
                try:
                    if driver.find_elements(By.CLASS_NAME, class_name):
                        return True
                except Exception:
                    pass
            time.sleep(0.2)
        return False

    def _navigate_to_url(self, driver: uc.Chrome, url: str, wait_classes: list[str]) -> bool:
        attempts = self.config.retries + 1
        for attempt in range(1, attempts + 1):
            if self._should_stop():
                raise RuntimeError("Stop requested. Aborting scrape.")
            if not self._is_driver_alive(driver):
                if self._should_stop():
                    raise RuntimeError("Stop requested. Aborting scrape.")
                raise RuntimeError("Browser was closed. Aborting scrape.")

            try:
                driver.set_page_load_timeout(self.config.timeout_sec)
                driver.get(url)
            except TimeoutException:
                pass
            except WebDriverException:
                if self._should_stop():
                    raise RuntimeError("Stop requested. Aborting scrape.")
                raise RuntimeError("Browser was closed during navigation. Aborting scrape.")
            except Exception:
                if attempt == attempts:
                    return False
                continue

            if self._wait_for_any_class(driver, wait_classes, self.config.timeout_sec):
                self._polite_wait()
                return True

            if attempt < attempts:
                self.log(f"Retrying load ({attempt}/{attempts - 1}): {url}")

        return False

    def _get_total_pages(self, driver: uc.Chrome) -> int:
        if not self._navigate_to_url(
            driver,
            self._build_page_url(1),
            ["search-results-container", "listing-card-v2", "hui-pagination-root", "no-results"],
        ):
            return 1

        soup = BeautifulSoup(driver.page_source, "html.parser")
        pagination = soup.select_one("ul[da-id='hive-pagination']")
        if not pagination:
            return 1

        max_page = 1
        for elem in pagination.find_all(["a", "span"], attrs={"da-id": True}):
            da_id = elem.get("da-id", "")
            match = re.search(r"hui-pagination-btn-page-(\d+)", da_id)
            if match:
                max_page = max(max_page, int(match.group(1)))

        return max_page

    def _collect_listing_links(self, driver: uc.Chrome, total_pages: int) -> list[str]:
        links: list[str] = []
        if self.config.max_pages is not None:
            target_pages = max(1, self.config.max_pages)
            if total_pages < target_pages:
                self.log(
                    "Detected pages are lower than Max Pages; "
                    f"still attempting pages 1 to {target_pages}."
                )
        else:
            target_pages = max(1, total_pages)

        self.log(f"Collecting links from {target_pages} pages")

        for current_page in range(1, target_pages + 1):
            if self._should_stop():
                break

            page_url = self._build_page_url(current_page)
            if not self._navigate_to_url(
                driver,
                page_url,
                ["search-results-container", "listing-card-v2", "hui-pagination-root", "no-results"],
            ):
                self.error_count += 1
                self.log(f"Page {current_page}: failed to load")
                continue

            soup = BeautifulSoup(driver.page_source, "html.parser")
            container = soup.find("div", class_="search-results-container")
            page_links: list[str] = []
            if container:
                # Only use the primary results block; ignore recommendation widgets.
                main_results_root = container.find(
                    "div", attrs={"da-id": "search-result-root"}, recursive=False
                )
                if main_results_root is None:
                    main_results_root = container.find("div", attrs={"da-id": "search-result-root"})

                cards = []
                if main_results_root is not None:
                    cards = main_results_root.find_all(
                        "div",
                        attrs={"da-id": "parent-listing-card-v2-regular"},
                        recursive=False,
                    )
                    if not cards:
                        cards = main_results_root.find_all(
                            "div", attrs={"da-id": "parent-listing-card-v2-regular"}
                        )

                for card in cards:
                    if card.find_parent("div", attrs={"da-id": "recommendation-widget"}):
                        continue
                    footer = card.find("a", class_="card-footer")
                    if footer and footer.get("href"):
                        page_links.append(footer["href"])

            if page_links:
                self.log(f"Page {current_page}/{target_pages}: {len(page_links)} listings")
            else:
                self.log(f"Page {current_page}/{target_pages}: 0 listings")

            links.extend(page_links)
            self.progress(
                {
                    "stage": "pages",
                    "current_page": current_page,
                    "total_pages": target_pages,
                    "listings_processed": 0,
                    "total_listings": 0,
                    "error_count": self.error_count,
                    "elapsed": int(time.time() - self.start_time),
                }
            )

        return list(dict.fromkeys(links))

    def _extract_listing_row(self, driver: uc.Chrome, url: str, query_districts: str) -> dict:
        agent_phone = reveal_and_extract_agent_phone(
            driver=driver,
            timeout_sec=self.config.timeout_sec,
            stop_requested=self._should_stop,
        )
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        asking_price = None
        for selector in ['[da-id="price-amount"]', "h2.amount"]:
            elem = soup.select_one(selector)
            if elem:
                asking_price = elem.get_text(strip=True)
                break

        psf = extract_psf_from_soup(soup)
        beds = extract_beds_from_soup(soup)
        baths = extract_baths_from_soup(soup)
        land_size = extract_land_size_from_soup(soup)

        nearest_mrt_distance = None
        mrt_elem = soup.select_one('[da-id="mrt-distance-text"], p.mrt-distance__text')
        if mrt_elem:
            nearest_mrt_distance = mrt_elem.get_text(strip=True)

        tenure = None
        tenure_elem = soup.select_one(".meta-table__item__wrapper__value")
        if tenure_elem:
            tenure = tenure_elem.get_text(strip=True)

        agent_name = None
        for selector in [".agent-name", ".listed-by__agent-name"]:
            agent_elem = soup.select_one(selector)
            if agent_elem:
                agent_name = agent_elem.get_text(strip=True)
                break

        district = query_districts
        address_elem = soup.select_one('[da-id="property-address"]')
        if address_elem and address_elem.get_text(strip=True):
            district = address_elem.get_text(strip=True)

        return {
            "URL": url,
            "District": district,
            "Asking Price": asking_price,
            "Beds": beds,
            "Baths": baths,
            "PSF": psf,
            "Nearest MRT + Distance": nearest_mrt_distance,
            "Land Size": land_size,
            "Tenure": tenure,
            "Agent Name": agent_name,
            "Agent Phone Number": agent_phone,
        }

    def run(self) -> dict:
        self.start_time = time.time()
        driver = self._create_driver()

        try:
            login_commercialguru(
                driver=driver,
                timeout_sec=self.config.timeout_sec,
                log_callback=self.log,
                stop_requested=self._should_stop,
            )

            total_pages = self._get_total_pages(driver)
            self.log(f"Detected total pages: {total_pages}")

            links = self._collect_listing_links(driver, total_pages)
            total = len(links)
            self.log(f"Total unique listings: {total}")

            query_districts = self._get_query_districts()
            self.log(f"Excel columns: {', '.join(STANDARD_CSV_FIELDS)}")
            self.log(f"District from query: {query_districts or '(empty)'}")

            processed = 0
            workbook = Workbook()
            sheet = workbook.active
            sheet.title = "Listings"
            sheet.append(STANDARD_CSV_FIELDS)

            for link in links:
                if self._should_stop():
                    self.log("Stop requested. Exiting now.")
                    break

                if not self._navigate_to_url(driver, link, ["amount", "price-amount", "amenities"]):
                    self.error_count += 1
                    processed += 1
                    self.log(f"[{processed}/{total}] failed to load {link}")
                    self.progress(
                        {
                            "stage": "listings",
                            "current_page": 0,
                            "total_pages": total_pages,
                            "listings_processed": processed,
                            "total_listings": total,
                            "error_count": self.error_count,
                            "elapsed": int(time.time() - self.start_time),
                        }
                    )
                    continue

                row = self._extract_listing_row(driver, link, query_districts)
                sheet.append([row.get(field) for field in STANDARD_CSV_FIELDS])

                processed += 1
                self.log(f"[{processed}/{total}] {link}")
                self.progress(
                    {
                        "stage": "listings",
                        "current_page": 0,
                        "total_pages": total_pages,
                        "listings_processed": processed,
                        "total_listings": total,
                        "error_count": self.error_count,
                        "elapsed": int(time.time() - self.start_time),
                    }
                )

            workbook.save(self.config.output_csv)

            return {
                "total_pages": total_pages,
                "total_links": total,
                "processed": processed,
                "errors": self.error_count,
                "elapsed": int(time.time() - self.start_time),
            }
        finally:
            try:
                if self._is_driver_alive(driver):
                    driver.quit()
            except Exception:
                pass
            with self._driver_lock:
                self._driver = None
