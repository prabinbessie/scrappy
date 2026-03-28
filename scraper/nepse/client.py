from __future__ import annotations

from typing import Any

from scraper.config import NEPSE_VERIFY_SSL


class NepseDataClient:
    def __init__(self, verify_ssl: bool = NEPSE_VERIFY_SSL) -> None:
        try:
            from nepse_scraper import NepseScraper
        except ImportError as error:
            raise RuntimeError(
                "Missing dependency 'nepse-scraper'. Install with: pip install nepse-scraper"
            ) from error

        self._client = NepseScraper(verify_ssl=verify_ssl)

    def fetch_market_status(self) -> dict[str, Any]:
        market_open = self._client.is_market_open()
        return {"isOpen": "OPEN" if market_open else "CLOSE"}

    def fetch_market_summary(self) -> dict[str, Any]:
        result = self._client.get_market_summary()
        return result if isinstance(result, dict) else {}

    def fetch_today_price(self) -> list[dict[str, Any]]:
        result = self._client.get_today_price()
        if not isinstance(result, list):
            return []
        return [item for item in result if isinstance(item, dict)]

    def fetch_company_disclosures(self) -> dict[str, Any]:
        result = self._client.get_company_disclosures()
        if isinstance(result, dict):
            return result

        if isinstance(result, list):
            return {"companyNews": result, "exchangeMessages": []}

        return {"companyNews": [], "exchangeMessages": []}
