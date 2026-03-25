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


def _dropdown_is_expanded(elem: object) -> bool:
    try:
        aria_expanded = (elem.get_attribute("aria-expanded") or "").lower()  # type: ignore[attr-defined]
    except Exception:
        return False

    return aria_expanded == "true"


def _find_dropdown_trigger(driver: uc.Chrome) -> Optional[object]:
    selector_candidates = [
        '[da-id="other-enquiry-dropdown"]',
        '.extend-view-trigger-point',
        '.actionable-link.contact-button-root.extend-view-trigger-point',
    ]

    try:
        for selector in selector_candidates:
            dropdown = _first_interactable(driver.find_elements(By.CSS_SELECTOR, selector))
            if dropdown is not None:
                return dropdown
    except WebDriverException:
        pass

    try:
        return _first_interactable(
            driver.find_elements(
                By.XPATH,
                "//*[contains(@class,'action-text') and normalize-space(.)='Other ways to enquire']/ancestor::*[@da-id='other-enquiry-dropdown'][1]",
            )
        )
    except Exception:
        return None


def _find_phone_button(driver: uc.Chrome) -> Optional[object]:
    selector_candidates = [
        '[da-id="enquiry-widget-phone-btn"]',
        '[da-id="enquiry-widget-phone-btn"] .action-text',
    ]

    try:
        for selector in selector_candidates:
            phone_btn = _first_interactable(driver.find_elements(By.CSS_SELECTOR, selector))
            if phone_btn is not None:
                return phone_btn
    except WebDriverException:
        pass

    try:
        return _first_interactable(
            driver.find_elements(
                By.XPATH,
                "//*[contains(@class,'action-text') and normalize-space(.)='View Phone Number']/ancestor::*[@da-id='enquiry-widget-phone-btn'][1]",
            )
        )
    except Exception:
        return None


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
    last_dropdown_click = 0.0
    post_dropdown_click_delay = 0.4

    while time.time() < deadline:
        if stop_requested():
            raise RuntimeError("Stop requested while extracting agent phone number.")

        # If the phone button is already available, click it directly.
        phone_btn = _find_phone_button(driver)
        if phone_btn is not None:
            # Let the dropdown finish animating/rendering before clicking.
            if (time.time() - last_dropdown_click) >= post_dropdown_click_delay:
                _click_with_fallback(driver, phone_btn)
        else:
            # Open "Other ways to enquire" only when the phone button is not visible.
            dropdown = _find_dropdown_trigger(driver)
            if dropdown is not None and (
                not _dropdown_is_expanded(dropdown) and (time.time() - last_dropdown_click) >= 0.6
            ):
                _click_with_fallback(driver, dropdown)
                last_dropdown_click = time.time()
                time.sleep(0.2)

        phone = _find_phone_on_page(driver)
        if phone:
            return phone

        time.sleep(0.35)

    return None