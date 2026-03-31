from pathlib import Path

from scraper.io import append_rows_to_csv
from scraper.ipo.sources import (
    fetch_all_ipo_source_records,
    parse_ipo_result_page,
    parse_nepselink_ipo_opening_page,
    parse_upcoming_ipo_page,
)


def test_parse_upcoming_ipo_page() -> None:
    html = """
    <div class="announcement-list">
      <div class="media">
        <small class="text-muted">2026-03-21</small>
        <div class="media-body">
          <a href="/NewsDetail.aspx?newsID=88888">Upper Tamakoshi Hydropower Limited is going to issue its 1,000,000 units IPO from 2026-03-22 to 2026-03-26</a>
        </div>
      </div>
    </div>
    """

    rows = parse_upcoming_ipo_page(html, "https://merolagani.com")
    assert len(rows) == 1
    assert rows[0]["source"] == "merolagani_upcoming"
    assert rows[0]["url"] == "https://merolagani.com/NewsDetail.aspx?newsID=88888"


def test_parse_ipo_result_page() -> None:
    html = """
    <div class="featured-news-list">
      <a href="/NewsDetail.aspx?newsID=77777"><h4>IPO allotment result published for Guardian Micro Life Insurance Limited</h4></a>
      <span class="text-org">2026-03-23</span>
    </div>
    """

    rows = parse_ipo_result_page(html, "https://merolagani.com")
    assert len(rows) == 1
    assert "allotment" in rows[0]["title"].lower()


def test_parse_nepselink_ipo_opening_page() -> None:
    html = """
    <table>
      <tr>
        <th>IPO Type</th><th>Company Name</th><th>Units</th><th>Price per Unit</th>
        <th>Minimum Apply</th><th>Open Date</th><th>Close Date</th><th>Status</th>
      </tr>
      <tr>
        <td>IPO-General</td><td>Kalinchowk Hydropower Limited</td><td>684750</td><td>100</td>
        <td>10</td><td>2082-12-22</td><td>2082-12-25</td><td>Coming Soon</td>
      </tr>
      <tr>
        <td>IPO-GENERAL</td><td>Shikhar Power Development Limited</td><td>1842600</td><td>100</td>
        <td>10</td><td>2082-11-17</td><td>2082-11-22</td><td>Closed</td>
      </tr>
    </table>
    """

    rows = parse_nepselink_ipo_opening_page(html, "https://nepselink.com/ipo-opening")

    assert len(rows) == 2
    assert rows[0]["source"] == "nepselink_ipo_opening"
    assert rows[0]["company"] == "Kalinchowk Hydropower Limited"
    assert rows[0]["issue_open_date"] == "2082-12-22"
    assert rows[0]["issue_status"] == "upcoming"
    assert rows[0]["announcement_date"] == "2082-12-22"
    assert rows[1]["issue_status"] == "closed"
    assert rows[1]["announcement_date"] == "2082-11-22"


def test_append_rows_to_csv(tmp_path: Path) -> None:
    target = tmp_path / "sample.csv"
    fieldnames = ["a", "b"]
    count = append_rows_to_csv(target, [{"a": 1, "b": 2}], fieldnames)

    assert count == 1
    assert target.exists()
    content = target.read_text(encoding="utf-8")
    assert "a,b" in content
    assert "1,2" in content


def test_append_rows_to_csv_dedupes_by_keys(tmp_path: Path) -> None:
    target = tmp_path / "sample_dedup.csv"
    fieldnames = ["scraped_at_utc", "symbol", "value"]

    first_count = append_rows_to_csv(
        target,
        [
            {"scraped_at_utc": "2026-03-29T07:30:00+00:00", "symbol": "ACLBSL", "value": 1},
            {"scraped_at_utc": "2026-03-29T07:30:00+00:00", "symbol": "NABIL", "value": 2},
        ],
        fieldnames,
        unique_key_fields=["scraped_at_utc", "symbol"],
    )
    second_count = append_rows_to_csv(
        target,
        [
            {"scraped_at_utc": "2026-03-29T07:30:00+00:00", "symbol": "ACLBSL", "value": 99},
            {"scraped_at_utc": "2026-03-29T08:00:00+00:00", "symbol": "ACLBSL", "value": 3},
        ],
        fieldnames,
        unique_key_fields=["scraped_at_utc", "symbol"],
    )

    assert first_count == 2
    assert second_count == 1

    lines = target.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 4
    assert "2026-03-29T07:30:00+00:00,ACLBSL,1" in lines
    assert "2026-03-29T08:00:00+00:00,ACLBSL,3" in lines


def test_fetch_all_ipo_source_records_tolerates_source_processing_error(monkeypatch) -> None:
    from scraper.ipo import sources

    def broken_upcoming() -> list[dict]:
        raise ValueError("unexpected parser failure")

    monkeypatch.setattr(sources, "fetch_upcoming_ipo_records", broken_upcoming)
    monkeypatch.setattr(sources, "fetch_nepselink_ipo_opening_records", lambda: [])
    monkeypatch.setattr(sources, "fetch_ipo_result_records", lambda: [])
    monkeypatch.setattr(sources, "fetch_nepse_ipo_disclosure_records", lambda client=None: [])

    bundle = fetch_all_ipo_source_records()

    assert bundle["upcoming_sources"] == []
    assert bundle["result_sources"] == []
    assert bundle["nepse_disclosure_sources"] == []
    assert bundle["nepselink_sources"] == []
