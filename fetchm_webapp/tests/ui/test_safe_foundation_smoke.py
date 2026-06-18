"""Opt-in Playwright smoke checks for FetchM WEB safe UI foundation.

Run against a live or temporary server:
    FETCHM_UI_TEST_URL=http://127.0.0.1:18080 python fetchm_webapp/tests/ui/test_safe_foundation_smoke.py
"""
from __future__ import annotations

import os
import sys

from playwright.sync_api import expect, sync_playwright


BASE_URL = os.environ.get("FETCHM_UI_TEST_URL", "http://127.0.0.1:18080/")
CHROME_PATH = os.environ.get("FETCHM_UI_CHROME_PATH", "/usr/bin/google-chrome")


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=CHROME_PATH)
        try:
            page = browser.new_page(viewport={"width": 390, "height": 844})
            page.goto(BASE_URL)
            page.wait_for_load_state("networkidle")
            menu = page.locator("#mobile-menu-toggle")
            expect(menu).to_be_visible()
            expect(page.locator("#primary-navigation")).not_to_be_visible()
            menu.click()
            expect(page.locator("#primary-navigation")).to_be_visible()
            assert menu.get_attribute("aria-expanded") == "true"
            page.keyboard.press("Escape")
            expect(page.locator("#primary-navigation")).not_to_be_visible()
            assert menu.get_attribute("aria-expanded") == "false"
            page.close()

            page = browser.new_page(viewport={"width": 1440, "height": 960})
            page.goto(BASE_URL)
            page.wait_for_load_state("networkidle")
            expect(page.locator("#mobile-menu-toggle")).not_to_be_visible()
            expect(page.locator("#primary-navigation")).to_be_visible()
            assert page.locator("#metadata-page-link").get_attribute("href") is None
            assert page.locator("#metadata-page-link").get_attribute("tabindex") == "-1"

            search = page.locator("#taxon-search-input")
            search.fill("salmonella")
            expect(page.locator(".search-result").first).to_be_visible(timeout=15000)
            assert search.get_attribute("role") == "combobox"
            assert search.get_attribute("aria-expanded") == "true"
            search.press("ArrowDown")
            assert search.get_attribute("aria-activedescendant")
            search.press("Enter")
            expect(page.locator("#selected-taxon-card")).to_be_visible(timeout=10000)
            expect(page.locator("#selected-taxon-name")).to_contain_text("Salmonella")
            assert page.locator("#metadata-page-link").get_attribute("href")
            assert page.locator("#metadata-page-link").get_attribute("aria-disabled") == "false"

            search.fill("salmonella")
            expect(page.locator(".search-result").first).to_be_visible(timeout=15000)
            search.fill("")
            expect(page.locator("#taxon-search-results")).not_to_be_visible(timeout=5000)
            assert search.get_attribute("aria-expanded") == "false"
            assert search.get_attribute("aria-activedescendant") is None

            search.fill("zzzxxyfetchmnomatch")
            expect(page.locator("#taxon-search-status")).to_contain_text("No matching taxa", timeout=15000)
            expect(page.locator("#taxon-search-results")).not_to_be_visible()
            page.close()
        finally:
            browser.close()
    print("safe foundation UI smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
