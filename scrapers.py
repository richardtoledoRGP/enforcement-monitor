import logging
import random
import re
import time
from collections import defaultdict
from urllib.parse import urljoin, urlparse

import feedparser
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

from models import EnforcementAction, ScrapeResult

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30

# Rate limiting: per-domain minimum gap + random jitter
MIN_DOMAIN_DELAY = 4.0   # minimum seconds between requests to the same domain
MAX_JITTER = 3.0          # up to this many extra random seconds
CROSS_DOMAIN_DELAY = 1.5  # minimum gap between any two requests (different domains)

# Retry on 403/429
MAX_RETRIES = 2
RETRY_BACKOFF_BASE = 8.0  # seconds; doubles each retry

# Track last request time per domain
_domain_last_request: dict[str, float] = defaultdict(float)
_global_last_request: float = 0.0


def _rate_limit(url: str):
    """Sleep enough to respect per-domain and global rate limits."""
    global _global_last_request
    domain = urlparse(url).netloc

    now = time.monotonic()

    # Per-domain delay
    domain_elapsed = now - _domain_last_request[domain]
    domain_wait = max(0, MIN_DOMAIN_DELAY - domain_elapsed)

    # Global delay (cross-domain)
    global_elapsed = now - _global_last_request
    global_wait = max(0, CROSS_DOMAIN_DELAY - global_elapsed)

    wait = max(domain_wait, global_wait) + random.uniform(0.5, MAX_JITTER)

    if wait > 0.1:
        logger.debug(f"Rate limit: waiting {wait:.1f}s before {domain}")
        time.sleep(wait)

    now = time.monotonic()
    _domain_last_request[domain] = now
    _global_last_request = now


def _fetch(url: str) -> cffi_requests.Response:
    """Fetch a URL using curl_cffi with Chrome TLS impersonation, rate limiting, and retries."""
    _rate_limit(url)

    for attempt in range(1 + MAX_RETRIES):
        response = cffi_requests.get(
            url,
            impersonate="chrome",
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            verify=False,
        )

        if response.status_code in (403, 429):
            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF_BASE * (2 ** attempt) + random.uniform(1, 4)
                logger.warning(
                    f"Got {response.status_code} from {urlparse(url).netloc}, "
                    f"retrying in {backoff:.0f}s (attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(backoff)
                continue
            else:
                logger.warning(
                    f"Got {response.status_code} from {urlparse(url).netloc} "
                    f"after {MAX_RETRIES} retries, giving up"
                )

        response.raise_for_status()
        return response


class BaseScraper:
    def matches_keywords(self, text: str, keywords: list[str]) -> bool:
        if not keywords:
            return True
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in keywords)

    def extract_penalty_amount(self, text: str) -> float:
        pattern = r'\$\s*([\d,]+(?:\.\d{2})?)\s*(?:million|mil\.?|M\b)?'
        match = re.search(pattern, text)
        if match:
            amount_str = match.group(1).replace(",", "")
            amount = float(amount_str)
            if re.search(r'million|mil\.?|M\b', match.group(0), re.IGNORECASE):
                amount *= 1_000_000
            return amount
        return 0.0

    def close(self):
        pass  # curl_cffi sessions are per-request; nothing to close


class RssScraper(BaseScraper):
    def scrape(self, source: dict) -> ScrapeResult:
        name = source["name"]
        url = source["url"]
        keywords = source.get("keywords", [])

        try:
            response = _fetch(url)
            feed = feedparser.parse(response.text)

            actions = []
            for entry in feed.entries:
                title = entry.get("title", "")
                link = entry.get("link", "")
                summary = entry.get("summary", "")
                published = entry.get("published", "")
                full_text = f"{title} {summary}"

                if not self.matches_keywords(full_text, keywords):
                    continue

                action = EnforcementAction(
                    source=name,
                    title=title.strip(),
                    url=link.strip(),
                    date=published,
                    raw_text=summary[:500],
                    penalty_amount=self.extract_penalty_amount(full_text),
                )
                actions.append(action)

            logger.info(f"[{name}] RSS: found {len(actions)} matching entries")
            return ScrapeResult(source_name=name, actions=actions, success=True)

        except Exception as e:
            logger.error(f"[{name}] RSS scrape failed: {e}")
            return ScrapeResult(source_name=name, actions=[], success=False, error=str(e))


class HtmlScraper(BaseScraper):
    def scrape(self, source: dict) -> ScrapeResult:
        name = source["name"]
        url = source["url"]
        selector = source.get("selector")
        keywords = source.get("keywords", [])

        if not selector:
            logger.warning(f"[{name}] No CSS selector configured — skipping")
            return ScrapeResult(
                source_name=name, actions=[], success=False,
                error="No CSS selector configured"
            )

        try:
            response = _fetch(url)
            soup = BeautifulSoup(response.text, "html.parser")
            elements = soup.select(selector)

            actions = []
            for el in elements:
                link_el = el if el.name == "a" else el.find("a")
                if not link_el:
                    continue

                title = link_el.get_text(strip=True)
                href = link_el.get("href", "")
                if not title or not href:
                    continue

                full_url = urljoin(url, href)
                full_text = el.get_text(" ", strip=True)

                if not self.matches_keywords(full_text, keywords):
                    continue

                date_text = self._extract_date_from_element(el, full_text)

                action = EnforcementAction(
                    source=name,
                    title=title[:300],
                    url=full_url,
                    date=date_text,
                    raw_text=full_text[:500],
                    penalty_amount=self.extract_penalty_amount(full_text),
                )
                actions.append(action)

            logger.info(f"[{name}] HTML: found {len(actions)} matching entries from {len(elements)} elements")
            return ScrapeResult(source_name=name, actions=actions, success=True)

        except Exception as e:
            logger.error(f"[{name}] HTML scrape failed: {e}")
            return ScrapeResult(source_name=name, actions=[], success=False, error=str(e))

    def _extract_date_from_element(self, el, text: str) -> str:
        time_el = el.find("time")
        if time_el:
            return time_el.get("datetime", time_el.get_text(strip=True))

        date_pattern = r'(\d{1,2}/\d{1,2}/\d{2,4}|\w+ \d{1,2},? \d{4}|\d{4}-\d{2}-\d{2})'
        match = re.search(date_pattern, text)
        return match.group(1) if match else ""


# ---------------------------------------------------------------------------
# Playwright-based scraper for JS-rendered pages
# ---------------------------------------------------------------------------

_browser = None
_playwright_ctx = None

PLAYWRIGHT_TIMEOUT = 20_000  # ms to wait for elements


def launch_browser():
    """Launch a shared headless Chromium instance for all Playwright sources."""
    global _browser, _playwright_ctx
    if _browser is not None:
        return
    from playwright.sync_api import sync_playwright
    _playwright_ctx = sync_playwright().start()
    _browser = _playwright_ctx.chromium.launch(headless=True)
    logger.info("Playwright browser launched")


def close_browser():
    """Close the shared browser instance."""
    global _browser, _playwright_ctx
    if _browser is not None:
        _browser.close()
        _browser = None
    if _playwright_ctx is not None:
        _playwright_ctx.stop()
        _playwright_ctx = None
        logger.info("Playwright browser closed")


class PlaywrightScraper(BaseScraper):
    # JS snippet to extract links from shadow DOM (Coveo, web components, etc.)
    # Filters to only links inside atomic-result elements (skips navigation)
    SHADOW_DOM_EXTRACT_JS = """() => {
        function findLinks(root, depth) {
            if (depth > 10) return [];
            let links = [];
            for (let el of root.querySelectorAll('*')) {
                if (el.tagName === 'A' && el.href && el.textContent.trim()) {
                    const href = el.href;
                    if (href.includes('.pdf') || href.includes('enforcement') ||
                        href.includes('order') || href.includes('/content/dam/')) {
                        links.push({text: el.textContent.trim().substring(0, 300), href: href});
                    }
                }
                if (el.shadowRoot) {
                    links = links.concat(findLinks(el.shadowRoot, depth + 1));
                }
            }
            return links;
        }
        const iface = document.querySelector('atomic-search-interface');
        return iface ? findLinks(iface, 0) : findLinks(document, 0);
    }"""

    def scrape(self, source: dict) -> ScrapeResult:
        name = source["name"]
        url = source["url"]
        selector = source.get("selector")
        wait_for = source.get("wait_for", selector)
        actions = source.get("actions", [])
        keywords = source.get("keywords", [])
        use_shadow_dom = source.get("shadow_dom", False)
        use_frames = source.get("use_frames", False)

        if not selector:
            logger.warning(f"[{name}] No CSS selector configured — skipping")
            return ScrapeResult(
                source_name=name, actions=[], success=False,
                error="No CSS selector configured"
            )

        if _browser is None:
            return ScrapeResult(
                source_name=name, actions=[], success=False,
                error="Browser not launched (use --with-browser)"
            )

        page = None
        try:
            _rate_limit(url)
            context = _browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                ignore_https_errors=True,
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)

            # Wait for JS-rendered content to appear
            if wait_for:
                try:
                    page.wait_for_selector(wait_for, timeout=PLAYWRIGHT_TIMEOUT)
                except Exception:
                    logger.warning(f"[{name}] Timed out waiting for '{wait_for}', proceeding with current content")

            # Execute form interaction steps if defined
            for action in actions:
                self._execute_action(page, action, name)

            # Shadow DOM extraction (for Coveo, web components)
            if use_shadow_dom:
                return self._extract_shadow_dom(page, source)

            # Frame extraction (for embedded iframes like Looker Studio)
            if use_frames:
                return self._extract_from_frames(page, source)

            # Standard extraction: get rendered HTML and parse with BeautifulSoup
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            elements = soup.select(selector)

            found_actions = []
            for el in elements:
                link_el = el if el.name == "a" else el.find("a")
                if not link_el:
                    continue

                title = link_el.get_text(strip=True)
                href = link_el.get("href", "")
                if not href:
                    continue

                full_url = urljoin(url, href)
                full_text = el.get_text(" ", strip=True)

                # Fall back to parent element text if link text is empty
                if not title:
                    title = full_text

                if not self.matches_keywords(full_text, keywords):
                    continue

                date_text = self._extract_date_from_element(el, full_text)

                found_actions.append(EnforcementAction(
                    source=name,
                    title=title[:300],
                    url=full_url,
                    date=date_text,
                    raw_text=full_text[:500],
                    penalty_amount=self.extract_penalty_amount(full_text),
                ))

            logger.info(f"[{name}] Playwright: found {len(found_actions)} matching entries from {len(elements)} elements")
            return ScrapeResult(source_name=name, actions=found_actions, success=True)

        except Exception as e:
            logger.error(f"[{name}] Playwright scrape failed: {e}")
            return ScrapeResult(source_name=name, actions=[], success=False, error=str(e))
        finally:
            if page:
                try:
                    page.context.close()
                except Exception:
                    pass

    def _extract_shadow_dom(self, page, source: dict) -> ScrapeResult:
        """Extract links from shadow DOM using Playwright's locator API (pierces shadow roots)."""
        name = source["name"]
        keywords = source.get("keywords", [])
        url = source["url"]

        # Playwright locators pierce shadow DOM automatically
        locators = page.locator('a[href*=".pdf"]').all()
        found_actions = []
        for loc in locators:
            href = loc.get_attribute("href") or ""
            if not href:
                continue
            # Get text from the link or its parent result element
            text = loc.text_content().strip()
            if not text:
                # Try to get title from the PDF filename
                text = href.split("/")[-1].replace("%20", " ").replace(".pdf", "").replace("_", " ")
            full_url = urljoin(url, href)
            if not self.matches_keywords(text, keywords):
                continue
            found_actions.append(EnforcementAction(
                source=name,
                title=text[:300],
                url=full_url,
                raw_text=text[:500],
                penalty_amount=self.extract_penalty_amount(text),
            ))
        logger.info(f"[{name}] Playwright (shadow DOM): found {len(found_actions)} links")
        return ScrapeResult(source_name=name, actions=found_actions, success=True)

    def _extract_from_frames(self, page, source: dict) -> ScrapeResult:
        """Extract data from embedded iframes (Looker Studio, etc.)."""
        name = source["name"]
        keywords = source.get("keywords", [])
        url = source["url"]

        found_actions = []
        for frame in page.frames:
            if "lookerstudio" in frame.url or "datastudio" in frame.url:
                try:
                    frame.wait_for_selector("div.cell", timeout=15_000)
                    cells = frame.query_selector_all("span.cell-value")
                    # Looker Studio groups cells in rows of 4: CaseNumber, Name, Url, Order
                    values = [c.text_content().strip() for c in cells]
                    for i in range(0, len(values) - 3, 4):
                        case_num = values[i]
                        entity_name = values[i + 1]
                        doc_url = values[i + 2]
                        order_type = values[i + 3]
                        title = f"{entity_name} — {order_type}" if order_type else entity_name
                        full_text = f"{case_num} {title}"
                        if not self.matches_keywords(full_text, keywords):
                            continue
                        found_actions.append(EnforcementAction(
                            source=name,
                            title=title[:300],
                            url=doc_url if doc_url.startswith("http") else urljoin(url, doc_url),
                            raw_text=full_text[:500],
                            penalty_amount=self.extract_penalty_amount(full_text),
                        ))
                except Exception as e:
                    logger.warning(f"[{name}] Frame extraction failed: {e}")
                break
        logger.info(f"[{name}] Playwright (frames): found {len(found_actions)} entries")
        return ScrapeResult(source_name=name, actions=found_actions, success=True)

    def _execute_action(self, page, action: dict, source_name: str):
        action_type = action.get("type", "")
        sel = action.get("selector", "")
        value = action.get("value", "")

        try:
            if action_type == "fill":
                page.fill(sel, value)
                logger.debug(f"[{source_name}] Filled '{sel}' with '{value}'")
            elif action_type == "click":
                page.click(sel)
                logger.debug(f"[{source_name}] Clicked '{sel}'")
            elif action_type == "wait":
                page.wait_for_selector(sel, timeout=PLAYWRIGHT_TIMEOUT)
                logger.debug(f"[{source_name}] Waited for '{sel}'")
            elif action_type == "select":
                page.select_option(sel, value)
                logger.debug(f"[{source_name}] Selected '{value}' in '{sel}'")
            elif action_type == "sleep":
                time.sleep(float(value or "2"))
            else:
                logger.warning(f"[{source_name}] Unknown action type: {action_type}")
        except Exception as e:
            logger.warning(f"[{source_name}] Action '{action_type}' on '{sel}' failed: {e}")

    def _extract_date_from_element(self, el, text: str) -> str:
        time_el = el.find("time")
        if time_el:
            return time_el.get("datetime", time_el.get_text(strip=True))

        date_pattern = r'(\d{1,2}/\d{1,2}/\d{2,4}|\w+ \d{1,2},? \d{4}|\d{4}-\d{2}-\d{2})'
        match = re.search(date_pattern, text)
        return match.group(1) if match else ""


# ---------------------------------------------------------------------------
# Scraper factory and entry point
# ---------------------------------------------------------------------------

def create_scraper(source_type: str) -> BaseScraper:
    scrapers = {
        "rss": RssScraper,
        "html": HtmlScraper,
        "playwright": PlaywrightScraper,
    }
    cls = scrapers.get(source_type)
    if not cls:
        raise ValueError(f"Unknown source type: {source_type}")
    return cls()


def scrape_source(source: dict) -> ScrapeResult:
    scraper = create_scraper(source["type"])
    try:
        return scraper.scrape(source)
    finally:
        scraper.close()
