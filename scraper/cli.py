from __future__ import annotations

import argparse

from scraper.ipo.service import scrape_ipo_to_json
from scraper.nepse.market import scrape_market_to_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrappy NEPSE scraper runner")
    parser.add_argument(
        "command",
        choices=["market", "ipo", "all"],
        help="Which scraping job to run",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command in {"market", "all"}:
        market_result = scrape_market_to_csv()
        print("Market scrape completed")
        print(f"- Source used: {market_result['price_source']}")
        print(f"- Market summary rows written: {market_result['summary_rows_written']}")
        print(f"- Price rows written: {market_result['price_rows_written']}")

    if args.command in {"ipo", "all"}:
        ipo_result = scrape_ipo_to_json()
        print("IPO scrape completed.")
        print(f"- Upcoming: {ipo_result['meta']['upcoming_count']}")
        print(f"- Open: {ipo_result['meta']['open_count']}")
        print(f"- Closed: {ipo_result['meta']['closed_count']}")
        print(f"- Results: {ipo_result['meta']['result_count']}")
        print(f"- Unknown: {ipo_result['meta']['unknown_count']}")


if __name__ == "__main__":
    main()
