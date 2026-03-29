from pathlib import Path

from scraper.io import append_rows_to_csv
from scraper.ipo.sources import parse_ipo_result_page, parse_upcoming_ipo_page


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
