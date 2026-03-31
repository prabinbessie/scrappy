from pathlib import Path

from scraper.ipo import service


def test_scrape_ipo_to_json_groups(monkeypatch, tmp_path: Path) -> None:
    fake_bundle = {
        "upcoming_sources": [
            {
                "title": "Kalinchowk Hydropower Limited IPO opens from 2099-01-01 to 2099-01-05",
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
        "nepselink_sources": [
            {
                "title": "Kalinchowk Hydropower Limited IPO-General",
                "details": "IPO-General Kalinchowk Hydropower Limited units 684750 price per unit 100 minimum apply 10 open 2099-01-01 close 2099-01-05 status Coming Soon",
                "announcement_date": None,
                "url": "https://nepselink.com/ipo-opening",
                "source": "nepselink_ipo_opening",
                "company": "Kalinchowk Hydropower Limited",
                "issue_type": "IPO-General",
                "issue_open_date": "2099-01-01",
                "issue_close_date": "2099-01-05",
                "min_quantity": 10,
                "total_quantity": 684750,
                "price_per_unit": 100,
                "issue_status": "upcoming",
            },
            {
                "title": "Shikhar Power Development Limited IPO-GENERAL",
                "details": "IPO-GENERAL Shikhar Power Development Limited units 1842600 price per unit 100 minimum apply 10 open 2082-11-17 close 2082-11-22 status Closed",
                "announcement_date": None,
                "url": "https://nepselink.com/ipo-opening",
                "source": "nepselink_ipo_opening",
                "company": "Shikhar Power Development Limited",
                "issue_type": "IPO-GENERAL",
                "issue_open_date": "2082-11-17",
                "issue_close_date": "2082-11-22",
                "min_quantity": 10,
                "total_quantity": 1842600,
                "price_per_unit": 100,
                "issue_status": "closed",
            },
        ],
    }

    monkeypatch.setattr(service, "fetch_all_ipo_source_records", lambda client=None: fake_bundle)
    monkeypatch.setattr(service, "IPO_FEED_JSON", tmp_path / "ipo_feed.json")

    payload = service.scrape_ipo_to_json()

    assert payload["meta"]["upcoming_count"] == 1
    assert payload["meta"]["closed_count"] >= 1
    assert payload["meta"]["result_count"] == 1
    assert payload["meta"]["sources"]["nepselink_ipo_opening"] == 2
    assert (tmp_path / "ipo_feed.json").exists()


def test_scrape_ipo_to_json_deduplicates_quantity_format(monkeypatch, tmp_path: Path) -> None:
    fake_bundle = {
        "upcoming_sources": [
            {
                "title": "Sample Hydro IPO opens from 2026-04-01 to 2026-04-05",
                "details": "Price per unit Rs 100",
                "announcement_date": "2026-03-31",
                "url": "https://example.com/a",
                "source": "source_a",
                "company": "Sample Hydro",
                "issue_type": "IPO",
                "issue_open_date": "2026-04-01",
                "issue_close_date": "2026-04-05",
                "total_quantity": 684750,
                "issue_status": "upcoming",
            },
            {
                "title": "Sample Hydro IPO opens from 2026-04-01 to 2026-04-05",
                "details": "Price per unit Rs 100",
                "announcement_date": "2026-03-31",
                "url": "https://example.com/b",
                "source": "source_b",
                "company": "Sample Hydro",
                "issue_type": "IPO",
                "issue_open_date": "2026-04-01",
                "issue_close_date": "2026-04-05",
                "total_quantity": 684750.0,
                "issue_status": "upcoming",
            },
        ],
        "result_sources": [],
        "nepse_disclosure_sources": [],
        "nepselink_sources": [],
    }

    monkeypatch.setattr(service, "fetch_all_ipo_source_records", lambda client=None: fake_bundle)
    monkeypatch.setattr(service, "IPO_FEED_JSON", tmp_path / "ipo_feed.json")

    payload = service.scrape_ipo_to_json()
    assert payload["meta"]["upcoming_count"] == 1


def test_scrape_ipo_to_json_normalizes_status_aliases(monkeypatch, tmp_path: Path) -> None:
    fake_bundle = {
        "upcoming_sources": [
            {
                "title": "Alpha IPO",
                "details": "IPO",
                "announcement_date": "2026-03-31",
                "url": "https://example.com/alpha",
                "source": "source_alpha",
                "company": "Alpha",
                "issue_type": "IPO",
                "total_quantity": 1000,
                "issue_status": "Coming Soon",
            },
            {
                "title": "Beta IPO",
                "details": "IPO",
                "announcement_date": "2026-03-31",
                "url": "https://example.com/beta",
                "source": "source_beta",
                "company": "Beta",
                "issue_type": "IPO",
                "issue_open_date": "2026-03-01",
                "issue_close_date": "2026-03-10",
                "total_quantity": 2000,
                "issue_status": "LIVE",
            },
            {
                "title": "Gamma IPO",
                "details": "IPO",
                "announcement_date": "2026-03-31",
                "url": "https://example.com/gamma",
                "source": "source_gamma",
                "company": "Gamma",
                "issue_type": "IPO",
                "issue_open_date": "2026-03-01",
                "issue_close_date": "2026-03-10",
                "total_quantity": 3000,
                "issue_status": "close",
            },
        ],
        "result_sources": [],
        "nepse_disclosure_sources": [],
        "nepselink_sources": [],
    }

    monkeypatch.setattr(service, "fetch_all_ipo_source_records", lambda client=None: fake_bundle)
    monkeypatch.setattr(service, "IPO_FEED_JSON", tmp_path / "ipo_feed.json")

    payload = service.scrape_ipo_to_json()
    assert payload["meta"]["upcoming_count"] == 1
    assert payload["meta"]["open_count"] == 1
    assert payload["meta"]["closed_count"] == 1
    assert payload["meta"]["unknown_count"] == 0
