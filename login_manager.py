from __future__ import annotations

import os
import time
from typing import Callable, Optional

import undetected_chromedriver as uc
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By

PG_HOME_URL = "https://www.propertyguru.com.sg/?"
CG_HOME_URL = "https://www.commercialguru.com.sg/?"

# You can override these with environment variables PG_LOGIN_EMAIL and PG_LOGIN_PASSWORD.
DEFAULT_LOGIN_EMAIL = "muhammadbilal.engr96@gmail.com"
DEFAULT_LOGIN_PASSWORD = "Bilal123$"


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


def _wait_for_element(
    driver: uc.Chrome,
    by: By,
    value: str,
    timeout: int,
    stop_requested: Callable[[], bool],
) -> Optional[object]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if stop_requested():
            raise RuntimeError("Stop requested before login completed.")
        try:
            elems = driver.find_elements(by, value)
            if elems:
                return elems[0]
        except WebDriverException:
            pass
        time.sleep(0.2)
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


def _open_login_form_and_wait_email(
    driver: uc.Chrome,
    timeout_sec: int,
    stop_requested: Callable[[], bool],
    log_callback: Callable[[str], None],
) -> Optional[object]:
    """Keep clicking login entry point until email field becomes available."""
    deadline = time.time() + timeout_sec
    email_selector = '[da-id="email-fld"]'
    login_selector = '[da-id="mega-menu-navbar-login-button"]'

    while time.time() < deadline:
        if stop_requested():
            raise RuntimeError("Stop requested before login completed.")

        email_inputs = driver.find_elements(By.CSS_SELECTOR, email_selector)
        email_input = _first_interactable(email_inputs)
        if email_input:
            return email_input

        login_buttons = driver.find_elements(By.CSS_SELECTOR, login_selector)
        login_btn = _first_interactable(login_buttons)
        if login_btn:
            log_callback("Clicking Login to open email form...")
            _click_with_fallback(driver, login_btn)

        time.sleep(0.4)

    return None


def _type_text(driver: uc.Chrome, elem: object, text: str) -> None:
    try:
        _click_with_fallback(driver, elem)
        elem.clear()  # type: ignore[attr-defined]
        elem.send_keys(text)  # type: ignore[attr-defined]
        return
    except Exception:
        pass

    # JS fallback for stubborn overlays/animated forms.
    try:
        driver.execute_script(
            """
            const el = arguments[0];
            const val = arguments[1];
            el.focus();
            el.value = '';
            el.value = val;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
            """,
            elem,
            text,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to type into login field: {exc}")


def _wait_for_interactable_element(
    driver: uc.Chrome,
    css_selector: str,
    timeout_sec: int,
    stop_requested: Callable[[], bool],
) -> Optional[object]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if stop_requested():
            raise RuntimeError("Stop requested before login completed.")
        elems = driver.find_elements(By.CSS_SELECTOR, css_selector)
        elem = _first_interactable(elems)
        if elem:
            return elem
        time.sleep(0.2)
    return None


def _is_logged_in(driver: uc.Chrome) -> bool:
    selectors = [
        '[da-id="mega-menu-navbar-root-profile-button"]',
        '[da-id="mega-menu-navbar-user-button"]',
        '[da-id="mega-menu-navbar-profile-button"]',
    ]
    for sel in selectors:
        try:
            if driver.find_elements(By.CSS_SELECTOR, sel):
                return True
        except Exception:
            continue
    return False


def login_propertyguru(
    driver: uc.Chrome,
    timeout_sec: int,
    log_callback: Callable[[str], None],
    stop_requested: Callable[[], bool],
) -> None:
    """Ensure PropertyGuru login is completed before scraping starts."""
    email = os.getenv("PG_LOGIN_EMAIL", DEFAULT_LOGIN_EMAIL)
    password = os.getenv("PG_LOGIN_PASSWORD", DEFAULT_LOGIN_PASSWORD)

    _login_on_site(
        driver=driver,
        timeout_sec=timeout_sec,
        log_callback=log_callback,
        stop_requested=stop_requested,
        home_url=PG_HOME_URL,
        site_label="PropertyGuru",
        email=email,
        password=password,
    )


def login_commercialguru(
    driver: uc.Chrome,
    timeout_sec: int,
    log_callback: Callable[[str], None],
    stop_requested: Callable[[], bool],
) -> None:
    """Ensure CommercialGuru login is completed before scraping starts."""
    email = os.getenv("CG_LOGIN_EMAIL", os.getenv("PG_LOGIN_EMAIL", DEFAULT_LOGIN_EMAIL))
    password = os.getenv("CG_LOGIN_PASSWORD", os.getenv("PG_LOGIN_PASSWORD", DEFAULT_LOGIN_PASSWORD))

    _login_on_site(
        driver=driver,
        timeout_sec=timeout_sec,
        log_callback=log_callback,
        stop_requested=stop_requested,
        home_url=CG_HOME_URL,
        site_label="CommercialGuru",
        email=email,
        password=password,
    )


def _login_on_site(
    driver: uc.Chrome,
    timeout_sec: int,
    log_callback: Callable[[str], None],
    stop_requested: Callable[[], bool],
    home_url: str,
    site_label: str,
    email: str,
    password: str,
) -> None:
    """Shared login routine for Guru sites using da-id based selectors."""

    if not email or not password:
        raise RuntimeError(f"Missing login credentials for {site_label}.")

    log_callback(f"Opening {site_label} home for login...")
    driver.get(home_url)

    if _is_logged_in(driver):
        log_callback("Already logged in. Proceeding to scrape.")
        return

    email_input = _open_login_form_and_wait_email(
        driver=driver,
        timeout_sec=timeout_sec,
        stop_requested=stop_requested,
        log_callback=log_callback,
    )
    if not email_input:
        raise RuntimeError("Email input not found during login.")
    _type_text(driver, email_input, email)

    continue_btn = _wait_for_interactable_element(
        driver=driver,
        css_selector='[da-id="continue-btn"]',
        timeout_sec=timeout_sec,
        stop_requested=stop_requested,
    )
    if not continue_btn:
        raise RuntimeError("Continue button not found during login.")
    _click_with_fallback(driver, continue_btn)

    password_input = _wait_for_interactable_element(
        driver=driver,
        css_selector='[da-id="password-fld"]',
        timeout_sec=timeout_sec,
        stop_requested=stop_requested,
    )
    if not password_input:
        raise RuntimeError("Password input not found during login.")
    _type_text(driver, password_input, password)

    login_submit_btn = _wait_for_interactable_element(
        driver=driver,
        css_selector='[da-id="login-btn"]',
        timeout_sec=timeout_sec,
        stop_requested=stop_requested,
    )
    if not login_submit_btn:
        raise RuntimeError("Log In button not found during login.")
    _click_with_fallback(driver, login_submit_btn)

    # Wait for login state to appear.
    wait_deadline = time.time() + timeout_sec
    while time.time() < wait_deadline:
        if stop_requested():
            raise RuntimeError("Stop requested before login completed.")
        if _is_logged_in(driver):
            log_callback("Login successful. Proceeding to scrape.")
            return
        time.sleep(0.2)

    raise RuntimeError("Login did not complete within timeout.")