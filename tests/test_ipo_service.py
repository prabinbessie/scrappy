from pathlib import Path

from scraper.ipo import service


def test_scrape_ipo_to_json_groups(monkeypatch, tmp_path: Path) -> None:
    fake_bundle = {
        "upcoming_sources": [
            {
                "title": "Crest Micro Life Insurance Limited IPO opens from 2099-01-01 to 2099-01-05",
                "details": "Minimum apply 10 units. Price per unit Rs 100",
                "announcement_date": "2026-03-20",
                "url": "https://merolagani.com/NewsDetail.aspx?newsID=66666",
                "source": "merolagani_upcoming",
            }
        ],
        "result_sources": [
            {
                "title": "Crest Micro Life Insurance Limited IPO allotment result published",
                "details": "Allotment result published",
                "announcement_date": "2026-03-21",
                "url": "https://merolagani.com/NewsDetail.aspx?newsID=55555",
                "source": "merolagani_results",
            }
        ],
        "nepse_disclosure_sources": [
            {
                "title": "NMB Debenture opens from 2020-01-01 to 2020-01-05",
                "details": "Debenture issue",
                "announcement_date": "2020-01-01",
                "url": "https://www.nepalstock.com.np/api/nots/security/fetchFiles?fileLocation=/media/notice.pdf",
                "source": "nepse_exchange_message",
            }
        ],
    }

    monkeypatch.setattr(service, "fetch_all_ipo_source_records", lambda client=None: fake_bundle)
    monkeypatch.setattr(service, "IPO_FEED_JSON", tmp_path / "ipo_feed.json")

    payload = service.scrape_ipo_to_json()

    assert payload["meta"]["upcoming_count"] == 1
    assert payload["meta"]["closed_count"] >= 1
    assert payload["meta"]["result_count"] == 1
    assert (tmp_path / "ipo_feed.json").exists()
