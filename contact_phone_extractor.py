from __future__ import annotations

import re
import time
from typing import Callable, Optional

import undetected_chromedriver as uc
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By

PHONE_REGEX = re.compile(r"(?:\+\d{1,3}\s*)?(?:\d[\s-]*){7,}")


def _is_interactable(elem: object) -> bool:
    try:
        return bool(elem.is_displayed() and elem.is_enabled())  # type: ignore[attr-defined]
    except Exception:
        return False


def _first_interactable(elems: list[object]) -> Optional[object]:
    for elem in elems:
        if _is_interactable(elem):
            return elem
    return None


def _click_with_fallback(driver: uc.Chrome, elem: object) -> None:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elem)
    except Exception:
        pass

    try:
        elem.click()  # type: ignore[attr-defined]
        return
    except Exception:
        pass

    try:
        driver.execute_script("arguments[0].click();", elem)
    except Exception:
        pass


def _normalize_phone(raw: str) -> Optional[str]:
    text = " ".join(raw.strip().split())
    if not text:
        return None

    match = PHONE_REGEX.search(text)
    if not match:
        return None

    phone = " ".join(match.group(0).split())
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 7:
        return None
    return phone


def _find_phone_on_page(driver: uc.Chrome) -> Optional[str]:
    selectors = [
        'a[href^="tel:"]',
        '[da-id="enquiry-widget-phone-btn"] .action-text',
        '.contact-button-root .action-text',
        '.actionable-link .action-text',
    ]

    for selector in selectors:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
        except WebDriverException:
            elems = []

        for elem in elems:
            try:
                text = elem.text or elem.get_attribute("textContent") or ""
            except Exception:
                continue

            phone = _normalize_phone(text)
            if phone:
                return phone

    return None


def reveal_and_extract_agent_phone(
    driver: uc.Chrome,
    timeout_sec: int,
    stop_requested: Callable[[], bool],
) -> Optional[str]:
    """Expand enquiry controls and extract visible agent phone number if present."""
    phone = _find_phone_on_page(driver)
    if phone:
        return phone

    deadline = time.time() + max(4, min(timeout_sec, 20))
    dropdown_selector = '[da-id="other-enquiry-dropdown"]'
    phone_btn_selector = '[da-id="enquiry-widget-phone-btn"]'

    while time.time() < deadline:
        if stop_requested():
            raise RuntimeError("Stop requested while extracting agent phone number.")

        dropdown = _first_interactable(driver.find_elements(By.CSS_SELECTOR, dropdown_selector))
        if dropdown is not None:
            _click_with_fallback(driver, dropdown)

        phone_btn = _first_interactable(driver.find_elements(By.CSS_SELECTOR, phone_btn_selector))
        if phone_btn is not None:
            _click_with_fallback(driver, phone_btn)

        phone = _find_phone_on_page(driver)
        if phone:
            return phone

        time.sleep(0.35)

    return None