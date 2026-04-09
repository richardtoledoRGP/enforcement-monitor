#!/usr/bin/env python3
"""
Regulatory Enforcement Action Monitor

Scrapes federal and state regulator websites for new enforcement actions
against financial institutions and sends email alerts.

Usage:
    python main.py                                  # Static sources only, with email
    python main.py --dry-run                        # Print results, don't email
    python main.py --with-browser --dry-run         # Include JS-rendered sources
    python main.py --source "OCC" --dry-run         # Test a single source
    python main.py --type playwright --dry-run      # Only run browser sources
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

from alert import send_alert
from diff import DiffEngine
from models import ScrapeResult
from scrapers import scrape_source, launch_browser, close_browser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_sources(config_path: str = "config/sources.yaml") -> list[dict]:
    path = Path(config_path)
    if not path.exists():
        logger.error(f"Source config not found: {config_path}")
        sys.exit(1)
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="Enforcement Action Monitor")
    parser.add_argument("--dry-run", action="store_true", help="Print results without sending email")
    parser.add_argument("--source", type=str, help="Scrape only the named source")
    parser.add_argument("--type", type=str, choices=["rss", "html", "playwright"], help="Only run sources of this type")
    parser.add_argument("--with-browser", action="store_true", help="Include playwright (JS-rendered) sources")
    parser.add_argument("--config", type=str, default="config/sources.yaml", help="Path to sources.yaml")
    parser.add_argument("--db", type=str, default="seen_actions.db", help="Path to SQLite database")
    args = parser.parse_args()

    load_dotenv()

    # Change to script directory so relative paths work
    os.chdir(Path(__file__).parent)

    sources = load_sources(args.config)
    logger.info(f"Loaded {len(sources)} source definitions")

    # Filter to single source if requested
    if args.source:
        sources = [s for s in sources if s["name"] == args.source]
        if not sources:
            logger.error(f"Source '{args.source}' not found in config")
            sys.exit(1)
        # Auto-enable browser if the requested source is a playwright source
        if any(s["type"] == "playwright" for s in sources):
            args.with_browser = True

    # Filter to enabled sources only
    sources = [s for s in sources if s.get("enabled", True)]

    # Filter by type if requested
    if args.type:
        sources = [s for s in sources if s["type"] == args.type]
        if args.type == "playwright":
            args.with_browser = True

    # Separate static and browser sources
    static_sources = [s for s in sources if s["type"] in ("rss", "html")]
    browser_sources = [s for s in sources if s["type"] == "playwright"]

    if not args.with_browser:
        browser_sources = []

    total_sources = len(static_sources) + len(browser_sources)
    logger.info(f"Scraping {total_sources} source(s): {len(static_sources)} static, {len(browser_sources)} browser")

    # Build source->category lookup for alert grouping
    all_sources = static_sources + browser_sources
    source_categories = {s["name"]: s.get("category", "other") for s in all_sources}

    results: list[ScrapeResult] = []
    all_actions = []
    counter = 0

    # Phase 1: Static sources (RSS + HTML)
    for source in static_sources:
        counter += 1
        logger.info(f"[{counter}/{total_sources}] Scraping {source['name']}...")
        result = scrape_source(source)
        results.append(result)
        all_actions.extend(result.actions)

    # Phase 2: Browser sources (Playwright)
    if browser_sources:
        logger.info("Launching browser for JS-rendered sources...")
        try:
            launch_browser()
            for source in browser_sources:
                counter += 1
                logger.info(f"[{counter}/{total_sources}] Scraping {source['name']} (browser)...")
                result = scrape_source(source)
                results.append(result)
                all_actions.extend(result.actions)
        finally:
            close_browser()

    total_found = len(all_actions)
    succeeded = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)
    logger.info(f"Scraping complete: {total_found} actions found across {succeeded} sources ({failed} failed)")

    # Deduplicate against previously seen actions
    diff = DiffEngine(args.db)
    new_actions = diff.check_new(all_actions)
    logger.info(f"New actions: {len(new_actions)} (of {total_found} total)")

    if args.dry_run:
        if new_actions:
            print(f"\n{'='*60}")
            print(f"  {len(new_actions)} NEW ENFORCEMENT ACTION(S) DETECTED")
            print(f"{'='*60}\n")
            for action in new_actions:
                penalty = f"  Penalty: ${action.penalty_amount:,.0f}" if action.penalty_amount else ""
                print(f"  [{action.source}] {action.title}")
                print(f"    Date: {action.date or 'N/A'}{penalty}")
                print(f"    URL:  {action.url}")
                print()
        else:
            print("\nNo new enforcement actions detected.\n")

        print(f"Source health: {succeeded}/{len(results)} succeeded")
        for r in results:
            if not r.success:
                print(f"  FAILED: {r.source_name} — {r.error}")

        diff.mark_seen(new_actions)
        logger.info(f"Database now contains {diff.count()} total records")
    else:
        diff.mark_seen(new_actions)
        logger.info(f"Database now contains {diff.count()} total records")

        if new_actions or failed > 0:
            send_alert(new_actions, results, source_categories)
        else:
            logger.info("No new actions and no failures — skipping email")

    diff.close()


if __name__ == "__main__":
    main()
