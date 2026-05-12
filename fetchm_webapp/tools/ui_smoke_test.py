#!/usr/bin/env python3
"""Browser smoke tests for FetchM Web.

Run against the deployed app:

    FETCHM_WEB_BASE_URL=https://fetchm.dulab206.xyz \
    FETCHM_WEB_TEST_USERNAME=... \
    FETCHM_WEB_TEST_PASSWORD=... \
    FETCHM_WEB_CHROMIUM_PATH=/usr/bin/google-chrome \
    python tools/ui_smoke_test.py

If credentials are omitted, the script only verifies public auth pages.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class SmokeConfig:
    base_url: str
    username: str
    password: str
    taxon_query: str
    screenshot_dir: str
    headless: bool
    chromium_path: str
    expect_admin: bool


def _load_config() -> SmokeConfig:
    return SmokeConfig(
        base_url=os.environ.get("FETCHM_WEB_BASE_URL", "https://fetchm.dulab206.xyz").rstrip("/"),
        username=os.environ.get("FETCHM_WEB_TEST_USERNAME", ""),
        password=os.environ.get("FETCHM_WEB_TEST_PASSWORD", ""),
        taxon_query=os.environ.get("FETCHM_WEB_TEST_TAXON_QUERY", "Klebsiella"),
        screenshot_dir=os.environ.get("FETCHM_WEB_UI_SCREENSHOT_DIR", "/tmp/fetchm-web-ui-smoke"),
        headless=os.environ.get("FETCHM_WEB_UI_HEADLESS", "1") != "0",
        chromium_path=os.environ.get("FETCHM_WEB_CHROMIUM_PATH", ""),
        expect_admin=os.environ.get("FETCHM_WEB_TEST_EXPECT_ADMIN", "0") == "1",
    )


def _require_playwright() -> None:
    try:
        import playwright  # noqa: F401
    except Exception as exc:  # pragma: no cover - exercised when dependency missing.
        raise SystemExit(
            "Playwright is not installed. Install it with `python -m pip install playwright` "
            "and then run `python -m playwright install chromium`."
        ) from exc


def _assert_visible(page, selector: str, label: str) -> None:
    locator = page.locator(selector).first
    if not locator.is_visible():
        raise AssertionError(f"Expected visible {label}: {selector}")


def main() -> int:
    _require_playwright()
    from playwright.sync_api import sync_playwright

    config = _load_config()
    os.makedirs(config.screenshot_dir, exist_ok=True)

    with sync_playwright() as p:
        launch_options = {"headless": config.headless}
        if config.chromium_path:
            launch_options["executable_path"] = config.chromium_path
        browser = p.chromium.launch(**launch_options)
        page = browser.new_page(viewport={"width": 1440, "height": 960})

        public_pages = ["/login", "/register", "/forgot-password"]
        for path in public_pages:
            page.goto(f"{config.base_url}{path}", wait_until="networkidle")
            _assert_visible(page, "body", path)
            page.screenshot(path=os.path.join(config.screenshot_dir, f"{path.strip('/') or 'home'}.png"), full_page=True)

        if not config.username or not config.password:
            print("Public UI smoke test passed. Authenticated checks skipped because credentials were not provided.")
            browser.close()
            return 0

        page.goto(f"{config.base_url}/login", wait_until="networkidle")
        page.fill("#login_identifier", config.username)
        page.fill("#password", config.password)
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")
        if "/login" in page.url:
            raise AssertionError("Login did not leave /login; check test credentials or rate limiting.")

        if config.expect_admin:
            page.goto(f"{config.base_url}/admin", wait_until="networkidle")
            if "/admin" not in page.url:
                raise AssertionError("Expected admin account to reach /admin.")
            _assert_visible(page, "body", "admin page")
            if "Admin" not in page.content():
                raise AssertionError("Admin page did not contain expected admin content.")
            page.screenshot(path=os.path.join(config.screenshot_dir, "admin.png"), full_page=True)
            browser.close()
            print("FetchM Web admin UI smoke test passed.")
            return 0

        _assert_visible(page, "#taxon-search-input", "taxon search")
        page.fill("#taxon-search-input", config.taxon_query)
        page.wait_for_selector("#taxon-search-results .search-result", timeout=15000)
        page.locator("#taxon-search-results .search-result").first.click()
        page.wait_for_timeout(500)

        metadata_link = page.locator("#metadata-page-link")
        if metadata_link.get_attribute("aria-disabled") == "true":
            raise AssertionError("Metadata link remained disabled after selecting a taxon.")
        metadata_link.click()
        page.wait_for_load_state("networkidle")
        _assert_visible(page, ".metadata-tabbar", "metadata tabbar")
        page.screenshot(path=os.path.join(config.screenshot_dir, "metadata.png"), full_page=True)

        sequence_link = page.locator("text=Go to Sequence Download Page").first
        if sequence_link.is_visible():
            sequence_link.click()
            page.wait_for_load_state("networkidle")
            _assert_visible(page, ".sequence-page", "sequence page")
            page.screenshot(path=os.path.join(config.screenshot_dir, "sequence.png"), full_page=True)

        browser.close()

    print("FetchM Web UI smoke test passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
