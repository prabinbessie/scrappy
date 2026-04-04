from scraper.nepse import market
from scraper.nepse.fallback import parse_sharesansar_live_table
from datetime import date, datetime


class DummyClient:
    def fetch_market_status(self):
        return {"isOpen": "OPEN"}

    def fetch_market_summary(self):
        return {"index": 2100.0, "pointChange": 5.5}

    def fetch_today_price(self):
        return []


def test_parse_sharesansar_live_table() -> None:
    html = """
    <table id="headFixed">
      <tbody>
        <tr>
                    <td>1</td><td>ACLBSL</td><td>100.5</td><td>1.2</td><td>1.2%</td><td>99</td><td>101</td><td>98</td><td>1,000</td><td>99.3</td>
        </tr>
      </tbody>
    </table>
    """
    rows = parse_sharesansar_live_table(html)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "ACLBSL"
    assert rows[0]["lastTradedPrice"] == 100.5


def test_market_fallback_source(monkeypatch, tmp_path) -> None:
    summary_path = tmp_path / "summary.csv"
    price_path = tmp_path / "price.csv"
    monkeypatch.setattr(market, "_is_within_trading_window", lambda _dt: True)
    monkeypatch.setattr(
        market,
        "_daily_market_paths",
        lambda _partition_date: (summary_path, price_path),
    )
    monkeypatch.setattr(
        market,
        "fetch_sharesansar_live_rows",
        lambda: [
            {
                "symbol": "ACLBSL",
                "securityName": "Aarambha Chautari Laghubitta Bittiya Sanstha Limited",
                "openPrice": 99,
                "highPrice": 101,
                "lowPrice": 98,
                "closePrice": 100,
                "lastTradedPrice": 100.5,
                "previousDayClosePrice": 99.3,
                "totalTradedQuantity": 1000,
                "totalTradedValue": 100500,
                "totalTrades": 20,
            }
        ],
    )

    result = market.scrape_market_to_csv(client=DummyClient())
    assert result["price_source"] == "sharesansar_fallback"
    assert result["price_rows_written"] == 1
    assert summary_path.exists()
    assert price_path.exists()


def test_daily_market_paths_rollover_month() -> None:
    dt_end_month = date(2026, 3, 31)
    summary_end, price_end = market._daily_market_paths(dt_end_month)

    dt_next_month = date(2026, 4, 1)
    summary_next, price_next = market._daily_market_paths(dt_next_month)

    assert str(summary_end).endswith("/nepse/2026/03/market_summary_2026-03-31.csv")
    assert str(price_end).endswith("/nepse/2026/03/today_price_2026-03-31.csv")
    assert str(summary_next).endswith("/nepse/2026/04/market_summary_2026-04-01.csv")
    assert str(price_next).endswith("/nepse/2026/04/today_price_2026-04-01.csv")


def test_market_scrape_creates_only_two_csv_files_per_day(monkeypatch, tmp_path) -> None:
    class ClientWithOfficialRows:
        def fetch_market_status(self):
            return {"isOpen": "OPEN"}

        def fetch_market_summary(self):
            return {
                "asOf": "2026-04-04",
                "index": 2100.0,
                "pointChange": 5.5,
            }

        def fetch_today_price(self):
            return [
                {
                    "businessDate": "2026-04-04",
                    "symbol": "ACLBSL",
                    "securityName": "Aarambha Chautari Laghubitta Bittiya Sanstha Limited",
                    "openPrice": 99,
                    "highPrice": 101,
                    "lowPrice": 98,
                    "closePrice": 100,
                    "lastTradedPrice": 100.5,
                    "previousDayClosePrice": 99.3,
                    "totalTradedQuantity": 1000,
                    "totalTradedValue": 100500,
                    "totalTrades": 20,
                }
            ]

    monkeypatch.setattr(market, "_is_within_trading_window", lambda _dt: True)
    monkeypatch.setattr(market, "NEPSE_DATA_DIR", tmp_path)

    market.scrape_market_to_csv(client=ClientWithOfficialRows())
    market.scrape_market_to_csv(client=ClientWithOfficialRows())

    files = sorted(path.name for path in tmp_path.rglob("*.csv"))
    assert files == [
        "market_summary_2026-04-04.csv",
        "today_price_2026-04-04.csv",
    ]


def test_trading_window_includes_1500_and_1505_only() -> None:
    at_1500 = datetime(2026, 4, 4, 15, 0, tzinfo=market.NPT_TZ)
    at_1505 = datetime(2026, 4, 4, 15, 5, tzinfo=market.NPT_TZ)
    at_1506 = datetime(2026, 4, 4, 15, 6, tzinfo=market.NPT_TZ)

    assert market._is_within_trading_window(at_1500)
    assert market._is_within_trading_window(at_1505)
    assert not market._is_within_trading_window(at_1506)
