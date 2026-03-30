from scraper.nepse import market
from scraper.nepse.fallback import parse_sharesansar_live_table


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
    monkeypatch.setattr(market, "_daily_market_paths", lambda _dt: (summary_path, price_path))
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
