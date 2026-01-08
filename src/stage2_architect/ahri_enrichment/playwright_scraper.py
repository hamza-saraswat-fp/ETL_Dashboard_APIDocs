"""
Playwright-based AHRI scraper with dual search modes

Supports:
1. Search by outdoor model number + indoor model number
2. Search by AHRI reference number
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)


# AHRI Program mapping for system types
AHRI_PROGRAM_MAP = {
    "AC": {"name": "Air Conditioning", "program_id": "101"},
    "Heat Pump": {"name": "Air-Source Heat Pumps", "program_id": "99"},
    "HP": {"name": "Air-Source Heat Pumps", "program_id": "99"},
}


class PlaywrightAHRIScraper:
    """
    AHRI certificate scraper using Playwright.

    Supports dual search modes:
    - 'model': Search by outdoor model number (returns all matching certificates)
    - 'ahri_number': Search by specific AHRI reference number
    """

    def __init__(self, cache_dir: str = "./cache/ahri", concurrency: int = 5, headless: bool = True):
        """
        Initialize scraper.

        Args:
            cache_dir: Directory for caching downloads
            concurrency: Max concurrent browser contexts
            headless: Run browser in headless mode
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.concurrency = concurrency
        self.headless = headless
        logger.info(f"Playwright scraper initialized (cache: {self.cache_dir}, concurrency: {concurrency})")

    async def search_by_outdoor_model(self, outdoor_model: str) -> Optional[Path]:
        """
        Search AHRI directory by outdoor model number.

        Downloads Excel file with ALL certificates for this outdoor model.

        Args:
            outdoor_model: Outdoor unit model number

        Returns:
            Path to downloaded CSV file, or None if failed
        """
        cache_file = self.cache_dir / f"ahri_model_{outdoor_model}.csv"

        if cache_file.exists():
            logger.info(f"Using cached AHRI data for model {outdoor_model}")
            return cache_file

        logger.info(f"Downloading AHRI certificates for model: {outdoor_model}")
        result = await self._download_ahri_file(outdoor_model, search_mode='model')

        if result[1] == 'success':
            return result[0]

        logger.error(f"Failed to download AHRI data for model {outdoor_model}: {result[1]}")
        return None

    async def search_by_ahri_number(self, ahri_number: str) -> Optional[Dict[str, Any]]:
        """
        Search AHRI directory by AHRI reference number.

        NEW APPROACH: Navigates directly to details page and scrapes data from HTML.
        No Excel download needed.

        Args:
            ahri_number: 9-digit AHRI reference number

        Returns:
            Dict with AHRI data (seer2, eer2, hspf2, capacity, etc.), or None if failed
        """
        import json

        cache_file = self.cache_dir / f"ahri_ref_{ahri_number}.json"

        if cache_file.exists():
            logger.info(f"Using cached AHRI data for AHRI# {ahri_number}")
            with open(cache_file, 'r') as f:
                return json.load(f)

        logger.info(f"Scraping AHRI certificate details for AHRI#: {ahri_number}")
        ahri_data = await self._scrape_ahri_details_page(ahri_number)

        if ahri_data:
            # Cache the scraped data
            with open(cache_file, 'w') as f:
                json.dump(ahri_data, f, indent=2)
            logger.info(f"Cached AHRI data for {ahri_number}")
            return ahri_data

        logger.error(f"Failed to scrape AHRI data for AHRI# {ahri_number}")
        return None

    async def search_by_outdoor_and_indoor_models(
        self,
        outdoor_model: str,
        indoor_model: str,
        system_type: str,
        furnace_model: Optional[str] = None
    ) -> Optional[Path]:
        """
        Search AHRI directory using advanced search with both outdoor and indoor models.

        This reduces results from thousands to <250, solving the non-member download limit.

        Args:
            outdoor_model: Outdoor unit model number
            indoor_model: Indoor unit model number
            system_type: System type ("AC", "Heat Pump", "HP")
            furnace_model: Optional furnace model number

        Returns:
            Path to downloaded CSV file, or None if failed
        """
        # Check cache first
        cache_key = f"{outdoor_model}_{indoor_model}_{system_type}".replace("/", "_")
        cache_file = self.cache_dir / f"ahri_combo_{cache_key}.csv"

        if cache_file.exists():
            logger.info(f"Using cached AHRI data for combo {outdoor_model} + {indoor_model}")
            return cache_file

        # Map system type to AHRI program
        if system_type not in AHRI_PROGRAM_MAP:
            logger.error(f"Unknown system type: {system_type}. Supported: {list(AHRI_PROGRAM_MAP.keys())}")
            return None

        program_info = AHRI_PROGRAM_MAP[system_type]
        program_id = program_info["program_id"]
        program_name = program_info["name"]

        logger.info(f"Searching AHRI {program_name} for: outdoor={outdoor_model}, indoor={indoor_model}")

        step = "init"
        try:
            async with async_playwright() as p:
                logger.debug(f"[{outdoor_model}+{indoor_model}] Launching browser")
                browser = await p.chromium.launch(
                    headless=self.headless,
                    slow_mo=500,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                    ]
                )

                step = "create_context"
                context = await browser.new_context(
                    accept_downloads=True,
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={'width': 1920, 'height': 1080},
                )

                # Apply stealth
                step = "apply_stealth"
                stealth = Stealth()
                await stealth.apply_stealth_async(context)

                step = "create_page"
                page = await context.new_page()

                # Enhanced stealth scripts
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false
                    });
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                """)

                # Navigate directly to program-specific search page
                step = "navigate_to_program"
                search_url = f"https://ahridirectory.org/search/{program_id}?searchMode=program"
                logger.debug(f"[{outdoor_model}+{indoor_model}] Navigating to {search_url}")
                await page.goto(search_url, wait_until="networkidle", timeout=60000)
                await asyncio.sleep(3)

                # Expand Search Criteria if collapsed
                step = "expand_search_criteria"
                try:
                    # Check if "Search Criteria" section is collapsed
                    search_criteria_button = await page.query_selector("button:has-text('Search Criteria')")
                    if search_criteria_button:
                        # Check if it's collapsed (aria-expanded="false" or similar)
                        is_expanded = await search_criteria_button.get_attribute("aria-expanded")
                        if is_expanded == "false":
                            logger.debug(f"[{outdoor_model}+{indoor_model}] Expanding Search Criteria")
                            await search_criteria_button.click()
                            await asyncio.sleep(1)
                except Exception as e:
                    logger.debug(f"[{outdoor_model}+{indoor_model}] Search Criteria may already be expanded: {e}")

                # Fill outdoor model number using Playwright's native fill() method
                # This properly triggers form framework state updates (React/Vue/Angular)
                step = "fill_outdoor_model"
                logger.debug(f"[{outdoor_model}+{indoor_model}] Filling outdoor model: {outdoor_model}")
                try:
                    outdoor_input = page.get_by_label("Outdoor Unit Model Number", exact=False)
                    await outdoor_input.fill(outdoor_model)
                    logger.debug(f"[{outdoor_model}+{indoor_model}] Outdoor model filled successfully")
                except Exception as e:
                    logger.error(f"[{outdoor_model}+{indoor_model}] Failed to fill outdoor model: {e}")
                    screenshot_path = self.cache_dir / f"failed_combo_{outdoor_model}_{indoor_model}_outdoor_field.png"
                    await page.screenshot(path=str(screenshot_path))
                    logger.error(f"Screenshot saved to {screenshot_path}")
                    await browser.close()
                    return None

                await asyncio.sleep(0.5)

                # Fill indoor model number using Playwright's native fill() method
                step = "fill_indoor_model"
                logger.debug(f"[{outdoor_model}+{indoor_model}] Filling indoor model: {indoor_model}")
                try:
                    indoor_input = page.get_by_label("Indoor Unit Model Number", exact=False)
                    await indoor_input.fill(indoor_model)
                    logger.debug(f"[{outdoor_model}+{indoor_model}] Indoor model filled successfully")
                except Exception as e:
                    logger.error(f"[{outdoor_model}+{indoor_model}] Failed to fill indoor model: {e}")
                    screenshot_path = self.cache_dir / f"failed_combo_{outdoor_model}_{indoor_model}_indoor_field.png"
                    await page.screenshot(path=str(screenshot_path))
                    logger.error(f"Screenshot saved to {screenshot_path}")
                    await browser.close()
                    return None

                await asyncio.sleep(0.5)

                # NOTE: We intentionally do NOT fill furnace model
                # Testing shows better results with just outdoor + indoor models
                # (furnace field can over-constrain the search)

                # DEBUG: Take screenshot after filling form
                debug_screenshot = self.cache_dir / f"debug_filled_form_{outdoor_model}_{indoor_model}.png"
                await page.screenshot(path=str(debug_screenshot))
                logger.debug(f"[{outdoor_model}+{indoor_model}] Debug screenshot saved: {debug_screenshot}")

                # Click Search button
                step = "click_search"
                logger.debug(f"[{outdoor_model}+{indoor_model}] Clicking Search button")
                try:
                    await page.click("button:has-text('Search')", timeout=5000)
                except Exception as e:
                    logger.warning(f"[{outdoor_model}+{indoor_model}] Fallback for Search button: {e}")
                    await page.evaluate("""
                        const buttons = Array.from(document.querySelectorAll('button'));
                        const searchBtn = buttons.find(b => b.textContent.toLowerCase().includes('search'));
                        if (searchBtn) searchBtn.click();
                    """)

                # Wait for search to complete (network request finishes)
                logger.debug(f"[{outdoor_model}+{indoor_model}] Waiting for search to complete")
                await page.wait_for_load_state('networkidle', timeout=30000)
                await asyncio.sleep(1)

                # Wait for results
                step = "wait_results"
                logger.debug(f"[{outdoor_model}+{indoor_model}] Waiting for results")
                try:
                    await page.wait_for_selector("text=/\\d{9}/", timeout=30000)
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"[{outdoor_model}+{indoor_model}] No results found: {e}")
                    screenshot_path = self.cache_dir / f"failed_combo_{cache_key}_no_results.png"
                    await page.screenshot(path=str(screenshot_path))
                    await browser.close()
                    return None

                # Results are ready - the Playwright fill() method triggers the form properly
                logger.info(f"[{outdoor_model}+{indoor_model}] ✓ Search complete, results displayed")

                # Click "Download Product List"
                step = "click_download_list"
                logger.debug(f"[{outdoor_model}+{indoor_model}] Clicking Download Product List")
                download_clicked = False
                for selector in ["button:has-text('Download Product List')", "a:has-text('Download Product List')"]:
                    try:
                        await page.click(selector, timeout=5000)
                        download_clicked = True
                        break
                    except:
                        continue

                if not download_clicked:
                    await page.evaluate("""
                        const buttons = Array.from(document.querySelectorAll('button, a'));
                        const downloadBtn = buttons.find(b =>
                            b.textContent.toLowerCase().includes('download') &&
                            b.textContent.toLowerCase().includes('product')
                        );
                        if (downloadBtn) downloadBtn.click();
                    """)

                await asyncio.sleep(1)

                # Wait for modal
                step = "wait_for_modal"
                logger.debug(f"[{outdoor_model}+{indoor_model}] Waiting for modal")
                try:
                    await page.wait_for_selector("button:has-text('Download Excel File')", timeout=10000, state="visible")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"[{outdoor_model}+{indoor_model}] Modal did not appear: {e}")
                    screenshot_path = self.cache_dir / f"failed_combo_{cache_key}_no_modal.png"
                    await page.screenshot(path=str(screenshot_path))
                    await browser.close()
                    return None

                # Download file
                step = "download_file"
                logger.info(f"[{outdoor_model}+{indoor_model}] Downloading Excel file")
                try:
                    async with page.expect_download(timeout=120000) as download_info:
                        await page.click("button:has-text('Download Excel File')")

                    download = await download_info.value
                    await download.save_as(cache_file)
                    logger.info(f"[{outdoor_model}+{indoor_model}] SUCCESS - Downloaded to {cache_file}")
                    await browser.close()
                    return cache_file

                except Exception as e:
                    logger.error(f"[{outdoor_model}+{indoor_model}] Download error: {e}")
                    screenshot_path = self.cache_dir / f"failed_combo_{cache_key}_download_error.png"
                    try:
                        await page.screenshot(path=str(screenshot_path))
                    except:
                        pass
                    await browser.close()
                    return None

        except Exception as e:
            logger.exception(f"[{outdoor_model}+{indoor_model}] EXCEPTION at step '{step}': {e}")
            return None

    async def _download_ahri_file(self, search_value: str, search_mode: str) -> Tuple[Optional[Path], str]:
        """
        Download AHRI certificate file.

        Args:
            search_value: Model number or AHRI reference number
            search_mode: 'model' or 'ahri_number'

        Returns:
            Tuple of (file_path, status)
        """
        if search_mode == 'model':
            cache_file = self.cache_dir / f"ahri_model_{search_value}.csv"
        else:
            cache_file = self.cache_dir / f"ahri_ref_{search_value}.csv"

        if cache_file.exists():
            return cache_file, "cached"

        step = "init"
        try:
            async with async_playwright() as p:
                logger.debug(f"[{search_value}] Launching browser")
                browser = await p.chromium.launch(
                    headless=self.headless,
                    slow_mo=500,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                    ]
                )

                step = "create_context"
                context = await browser.new_context(
                    accept_downloads=True,
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={'width': 1920, 'height': 1080},
                )

                # Apply stealth
                step = "apply_stealth"
                stealth = Stealth()
                await stealth.apply_stealth_async(context)

                step = "create_page"
                page = await context.new_page()

                # Enhanced stealth scripts
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false
                    });
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                """)

                # Navigate to AHRI
                step = "navigate"
                logger.debug(f"[{search_value}] Navigating to https://ahridirectory.org")
                await page.goto("https://ahridirectory.org", wait_until="networkidle", timeout=60000)
                await asyncio.sleep(3)

                # Select search mode radio button
                step = "select_search_mode"
                if search_mode == 'ahri_number':
                    logger.debug(f"[{search_value}] Clicking 'AHRI Reference #' radio button")
                    try:
                        # Try new label text first
                        await page.click("text=AHRI Reference #", timeout=5000)
                    except Exception:
                        try:
                            # Fallback: just "AHRI" text
                            await page.click("text=AHRI", timeout=5000)
                        except Exception as e:
                            logger.warning(f"[{search_value}] Fallback for AHRI radio: {e}")
                            await page.evaluate("""
                                const labels = Array.from(document.querySelectorAll('label'));
                                const ahriLabel = labels.find(l => l.textContent.includes('AHRI') && l.textContent.includes('Reference'));
                                if (ahriLabel) ahriLabel.click();
                            """)
                    await asyncio.sleep(1)
                else:  # model search
                    logger.debug(f"[{search_value}] Clicking 'Model #' radio button")
                    try:
                        await page.click("text=Model #", timeout=5000)
                    except Exception as e:
                        logger.warning(f"[{search_value}] Fallback for Model# radio: {e}")
                        await page.evaluate("""
                            const labels = Array.from(document.querySelectorAll('label'));
                            const modelLabel = labels.find(l => l.textContent.includes('Model'));
                            if (modelLabel) modelLabel.click();
                        """)
                    await asyncio.sleep(1)

                # Enter search value
                step = "enter_search_value"
                logger.debug(f"[{search_value}] Entering search value: {search_value}")
                # Use generic text input selector since specific IDs changed
                try:
                    await page.fill("input[type='text']", search_value, timeout=10000)
                except Exception:
                    # Fallback: try old selectors
                    if search_mode == 'ahri_number':
                        await page.fill("input#RefAHRIRefNum", search_value, timeout=10000)
                    else:
                        await page.fill("input#RefModNum", search_value, timeout=10000)
                await asyncio.sleep(1)

                # Click search
                step = "click_search"
                logger.debug(f"[{search_value}] Clicking search button")
                try:
                    await page.click("button#showSearchModal", timeout=5000)
                except Exception as e:
                    logger.warning(f"[{search_value}] Fallback for search button: {e}")
                    await page.evaluate("""
                        const button = document.querySelector('#showSearchModal') ||
                                      document.querySelector('button[type="submit"]');
                        if (button) button.click();
                    """)
                await asyncio.sleep(2)

                # Wait for results
                step = "wait_results"
                logger.debug(f"[{search_value}] Waiting for results")
                try:
                    await page.wait_for_selector("text=/\\d{9}/", timeout=30000)
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"[{search_value}] No results found: {e}")
                    screenshot_path = self.cache_dir / f"failed_{search_mode}_{search_value}_no_results.png"
                    await page.screenshot(path=str(screenshot_path))
                    await browser.close()
                    return None, "no_results"

                # Click "Download Product List"
                step = "click_download_list"
                logger.debug(f"[{search_value}] Clicking Download Product List")
                download_clicked = False
                for selector in ["button:has-text('Download Product List')", "a:has-text('Download Product List')"]:
                    try:
                        await page.click(selector, timeout=5000)
                        download_clicked = True
                        break
                    except:
                        continue

                if not download_clicked:
                    await page.evaluate("""
                        const buttons = Array.from(document.querySelectorAll('button, a'));
                        const downloadBtn = buttons.find(b =>
                            b.textContent.toLowerCase().includes('download') &&
                            b.textContent.toLowerCase().includes('product')
                        );
                        if (downloadBtn) downloadBtn.click();
                    """)

                await asyncio.sleep(1)

                # Wait for modal
                step = "wait_for_modal"
                logger.debug(f"[{search_value}] Waiting for modal")
                try:
                    await page.wait_for_selector("button:has-text('Download Excel File')", timeout=10000, state="visible")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"[{search_value}] Modal did not appear: {e}")
                    screenshot_path = self.cache_dir / f"failed_{search_mode}_{search_value}_no_modal.png"
                    await page.screenshot(path=str(screenshot_path))
                    await browser.close()
                    return None, "no_modal"

                # Download file
                step = "download_file"
                logger.info(f"[{search_value}] Downloading Excel file")
                try:
                    async with page.expect_download(timeout=120000) as download_info:
                        await page.click("button:has-text('Download Excel File')")

                    download = await download_info.value
                    await download.save_as(cache_file)
                    logger.info(f"[{search_value}] SUCCESS - Downloaded to {cache_file}")
                    await browser.close()
                    return cache_file, "success"

                except Exception as e:
                    logger.error(f"[{search_value}] Download error: {e}")
                    screenshot_path = self.cache_dir / f"failed_{search_mode}_{search_value}_download_error.png"
                    try:
                        await page.screenshot(path=str(screenshot_path))
                    except:
                        pass
                    await browser.close()
                    return None, "error"

        except Exception as e:
            logger.exception(f"[{search_value}] EXCEPTION at step '{step}': {e}")
            return None, f"error:{step}"

    def match_indoor_unit(self, outdoor_model: str, indoor_model: Optional[str], ahri_file: Path) -> Optional[Dict[str, Any]]:
        """
        Match indoor unit in downloaded AHRI certificate file.

        Args:
            outdoor_model: Outdoor unit model
            indoor_model: Indoor unit model (can be None)
            ahri_file: Path to downloaded AHRI CSV

        Returns:
            Dictionary with AHRI data, or None if no match
        """
        try:
            ahri_df = pd.read_excel(ahri_file, skiprows=1)
            ahri_df.columns = ahri_df.columns.str.strip()

            if len(ahri_df) == 0:
                logger.warning(f"AHRI file for {outdoor_model} has no certificates")
                return None

            # Find SEER2 column
            seer2_col = None
            for col in ahri_df.columns:
                if 'SEER2' in col or 'SEER 2' in col:
                    seer2_col = col
                    break

            if not seer2_col:
                logger.warning(f"No SEER2 column in AHRI file for {outdoor_model}")
                return None

            # If no indoor model, return first certificate
            if not indoor_model or 'Indoor Unit Model Number' not in ahri_df.columns:
                logger.info(f"No indoor model specified, returning first certificate")
                return self._extract_ahri_row_data(ahri_df.iloc[0], seer2_col)

            # Try exact match
            indoor_upper = indoor_model.upper()
            exact_match = ahri_df[ahri_df['Indoor Unit Model Number'].str.upper() == indoor_upper]
            if len(exact_match) > 0:
                logger.info(f"EXACT match: {indoor_model}")
                return self._extract_ahri_row_data(exact_match.iloc[0], seer2_col)

            # Try fuzzy similarity matching
            # PHILOSOPHY: If AHRI returned these results for our search, they're already filtered.
            # We just need to find the closest match, not decode complex wildcard patterns.
            from difflib import SequenceMatcher
            import re

            similarity_matches = []
            for idx, row in ahri_df.iterrows():
                ahri_indoor = str(row['Indoor Unit Model Number']).strip().upper()

                # Normalize both strings for comparison (remove wildcards and suffixes)
                ahri_normalized = re.sub(r'\*', '', ahri_indoor)  # Remove wildcards
                ahri_normalized = re.sub(r'\+[A-Z0-9]+.*$', '', ahri_normalized)  # Remove +SUFFIX

                our_normalized = indoor_upper

                # Calculate similarity ratio (0.0 to 1.0)
                similarity = SequenceMatcher(None, our_normalized, ahri_normalized).ratio()

                # Also check if our model is a substring of AHRI's model (common case)
                substring_bonus = 0.0
                if our_normalized in ahri_normalized or ahri_normalized in our_normalized:
                    substring_bonus = 0.1

                total_score = similarity + substring_bonus
                similarity_matches.append((total_score, similarity, row, ahri_indoor))

            if similarity_matches:
                # Sort by total score (similarity + substring bonus)
                similarity_matches.sort(key=lambda x: x[0], reverse=True)
                best_score, best_similarity, best_match, ahri_model = similarity_matches[0]

                # Use a reasonable threshold (0.7 = 70% similar)
                if best_similarity >= 0.7:
                    logger.info(f"FUZZY match (similarity={best_similarity:.2f}): {indoor_model} -> {ahri_model}")
                    return self._extract_ahri_row_data(best_match, seer2_col)
                else:
                    logger.warning(f"Best match below threshold: {indoor_model} -> {ahri_model} (similarity={best_similarity:.2f})")

            # If we get here, no good matches found

            logger.warning(f"NO MATCH for indoor: {indoor_model}")
            return None

        except Exception as e:
            logger.exception(f"Error matching AHRI for {outdoor_model}: {e}")
            return None

    async def _scrape_ahri_details_page(self, ahri_number: str) -> Optional[Dict[str, Any]]:
        """
        Scrape AHRI certificate details using the search interface.

        Uses the proper AHRI directory search (not direct URL) to find certificate,
        then parses the details page using DOM table selectors.

        Args:
            ahri_number: 9-digit AHRI reference number

        Returns:
            Dict with AHRI data (seer2, eer2, hspf2, capacity, etc.), or None if failed
        """
        step = "init"
        try:
            async with async_playwright() as p:
                logger.debug(f"[AHRI#{ahri_number}] Launching browser")
                browser = await p.chromium.launch(
                    headless=self.headless,
                    slow_mo=500,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                    ]
                )

                step = "create_context"
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={'width': 1920, 'height': 1080},
                )

                # Apply stealth
                step = "apply_stealth"
                stealth = Stealth()
                await stealth.apply_stealth_async(context)

                step = "create_page"
                page = await context.new_page()

                # Enhanced stealth scripts
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false
                    });
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                """)

                # Navigate to AHRI directory homepage
                step = "navigate_to_home"
                logger.info(f"[AHRI#{ahri_number}] Navigating to AHRI directory")
                await page.goto("https://ahridirectory.org", wait_until="networkidle", timeout=60000)
                await asyncio.sleep(3)

                # Click "AHRI Reference #" radio button
                step = "select_ahri_search_mode"
                logger.debug(f"[AHRI#{ahri_number}] Selecting AHRI Reference # search mode")
                try:
                    await page.click("text=AHRI Reference #", timeout=5000)
                except Exception:
                    try:
                        await page.click("text=AHRI", timeout=5000)
                    except Exception as e:
                        logger.warning(f"[AHRI#{ahri_number}] Fallback for AHRI radio: {e}")
                        await page.evaluate("""
                            const labels = Array.from(document.querySelectorAll('label'));
                            const ahriLabel = labels.find(l => l.textContent.includes('AHRI') && l.textContent.includes('Reference'));
                            if (ahriLabel) ahriLabel.click();
                        """)
                await asyncio.sleep(1)

                # Enter AHRI number in search field
                step = "enter_ahri_number"
                logger.debug(f"[AHRI#{ahri_number}] Entering AHRI number")
                try:
                    await page.fill("input[type='text']", ahri_number, timeout=10000)
                except Exception:
                    await page.fill("input#RefAHRIRefNum", ahri_number, timeout=10000)
                await asyncio.sleep(1)

                # Click search button - this will open certificate in a new tab
                step = "click_search"
                logger.debug(f"[AHRI#{ahri_number}] Clicking search button (will open new tab)")

                # Set up listener for new page/tab
                async with page.expect_popup() as popup_info:
                    try:
                        await page.click("button#showSearchModal", timeout=5000)
                    except Exception as e:
                        logger.warning(f"[AHRI#{ahri_number}] Fallback for search button: {e}")
                        await page.evaluate("""
                            const button = document.querySelector('#showSearchModal') ||
                                          document.querySelector('button[type="submit"]');
                            if (button) button.click();
                        """)

                # Wait for the new tab to open
                step = "wait_for_new_tab"
                logger.debug(f"[AHRI#{ahri_number}] Waiting for certificate tab to open")
                try:
                    new_page = await popup_info.value
                    await new_page.wait_for_load_state("networkidle", timeout=30000)
                    await asyncio.sleep(2)
                    logger.debug(f"[AHRI#{ahri_number}] Certificate tab opened successfully")
                except Exception as e:
                    logger.error(f"[AHRI#{ahri_number}] No new tab opened - AHRI number may be invalid/discontinued: {e}")
                    screenshot_path = self.cache_dir / f"failed_ahri_{ahri_number}_no_popup.png"
                    await page.screenshot(path=str(screenshot_path))
                    logger.info(f"[AHRI#{ahri_number}] Screenshot saved to {screenshot_path}")
                    await browser.close()
                    return None

                # Check if we got a 404 or error message on the new tab
                step = "check_for_errors"
                page_text = await new_page.inner_text("body")
                if '404' in page_text or 'not found' in page_text.lower() or 'Invalid Reference Number' in page_text:
                    logger.error(f"[AHRI#{ahri_number}] Certificate not found (404 or invalid)")
                    screenshot_path = self.cache_dir / f"failed_ahri_{ahri_number}_404.png"
                    await new_page.screenshot(path=str(screenshot_path))
                    await new_page.close()
                    await browser.close()
                    return None

                # Now we're on the certificate details page - extract data using DOM selectors
                step = "extract_data_from_tables"
                logger.debug(f"[AHRI#{ahri_number}] Extracting data from tables")

                # Switch to using new_page instead of page for extraction
                page = new_page

                # Initialize data dict
                ahri_data = {
                    'ahri_ref': ahri_number,
                    'seer2': None,
                    'eer2': None,
                    'hspf2': None,
                    'capacity': None,
                    'tonnage': None,
                    'indoor_model': None,
                    'outdoor_model': None,
                    'furnace_model': None,
                }

                # Get all tables
                tables = await page.query_selector_all('table')
                logger.debug(f"[AHRI#{ahri_number}] Found {len(tables)} tables on page")

                # Parse each table
                for table in tables:
                    rows = await table.query_selector_all('tbody tr')
                    for row in rows:
                        cells = await row.query_selector_all('td')
                        if len(cells) < 2:
                            continue

                        label_elem = cells[0]
                        value_elem = cells[1]

                        label = await label_elem.inner_text()
                        value = await value_elem.inner_text()
                        value = value.strip()

                        # Skip empty values
                        if not value:
                            continue

                        # Extract SEER2
                        if 'SEER2' in label and 'Appendix M1' in label:
                            try:
                                ahri_data['seer2'] = float(value)
                                logger.debug(f"[AHRI#{ahri_number}] Found SEER2: {ahri_data['seer2']}")
                            except ValueError:
                                pass

                        # Extract EER2
                        elif 'EER2' in label and '95F' in label and 'Appendix M1' in label:
                            try:
                                ahri_data['eer2'] = float(value)
                                logger.debug(f"[AHRI#{ahri_number}] Found EER2: {ahri_data['eer2']}")
                            except ValueError:
                                pass

                        # Extract HSPF2
                        elif 'HSPF2' in label and 'Region IV' in label and 'Appendix M1' in label:
                            try:
                                ahri_data['hspf2'] = float(value)
                                logger.debug(f"[AHRI#{ahri_number}] Found HSPF2: {ahri_data['hspf2']}")
                            except ValueError:
                                pass

                        # Extract Cooling Capacity
                        elif 'Cooling Capacity' in label and '95F' in label and 'btuh' in label and 'Appendix M1' in label:
                            try:
                                capacity = int(value)
                                ahri_data['capacity'] = capacity
                                ahri_data['tonnage'] = round(capacity / 12000, 1)
                                logger.debug(f"[AHRI#{ahri_number}] Found Capacity: {capacity} ({ahri_data['tonnage']} tons)")
                            except ValueError:
                                pass

                        # Extract Indoor Unit Model Number
                        elif 'Indoor Unit Model Number' in label and 'Brand' not in label:
                            ahri_data['indoor_model'] = value
                            logger.debug(f"[AHRI#{ahri_number}] Found Indoor Model: {value}")

                        # Extract Outdoor Unit Model Number
                        elif 'Outdoor Unit Model Number' in label and 'Brand' not in label:
                            ahri_data['outdoor_model'] = value
                            logger.debug(f"[AHRI#{ahri_number}] Found Outdoor Model: {value}")

                        # Extract Furnace Model Number
                        elif 'Furnace Model Number' in label:
                            ahri_data['furnace_model'] = value
                            logger.debug(f"[AHRI#{ahri_number}] Found Furnace Model: {value}")

                logger.info(f"[AHRI#{ahri_number}] Successfully extracted data: SEER2={ahri_data['seer2']}, EER2={ahri_data['eer2']}, HSPF2={ahri_data['hspf2']}, Capacity={ahri_data['capacity']}")

                await browser.close()
                return ahri_data

        except Exception as e:
            logger.exception(f"[AHRI#{ahri_number}] EXCEPTION at step '{step}': {e}")
            return None

    def _extract_ahri_row_data(self, row: pd.Series, seer2_col: str) -> Dict[str, Any]:
        """Extract AHRI data from matched row"""
        # Convert pandas/numpy types to Python native types for JSON serialization
        capacity = row.get('AHRI CERTIFIED RATINGS - Cooling Capacity (95F), btuh (Appendix M1)')
        capacity = int(capacity) if pd.notna(capacity) else None
        tonnage = round(capacity / 12000, 1) if capacity else None

        # Helper to convert pandas types to Python types
        def to_python_type(val):
            if pd.isna(val):
                return None
            if isinstance(val, (pd.Int64Dtype, pd.Float64Dtype)) or hasattr(val, 'item'):
                return val.item()
            return float(val) if isinstance(val, (int, float)) else val

        return {
            'ahri_ref': row.get('AHRI Ref. #'),
            'seer2': to_python_type(row.get(seer2_col)),
            'eer2': to_python_type(row.get('AHRI CERTIFIED RATINGS - EER2 (95F) (Appendix M1)')),
            'hspf2': to_python_type(row.get('AHRI CERTIFIED RATINGS - HSPF2 (Region IV) (Appendix M1)')),
            'capacity': capacity,
            'tonnage': tonnage,
            'indoor_model': row.get('Indoor Unit Model Number'),
            'furnace_model': row.get('Furnace Model Number'),
        }
