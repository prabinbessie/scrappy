from scraper.ipo.classifier import (
    classify_ipo_entry,
    detect_issue_type,
    derive_issue_status,
    extract_quantities_and_price,
)


def test_detect_issue_type_debenture() -> None:
    text = "Nabil Bank Limited announces debenture issue for general public"
    assert detect_issue_type(text) == "debenture"


def test_extract_quantities_and_price() -> None:
    text = "Minimum apply 10 units, maximum apply 1000 units, price per unit Rs. 100"
    min_quantity, max_quantity, total_quantity, price = extract_quantities_and_price(text)

    assert min_quantity == 10
    assert max_quantity == 1000
    assert total_quantity == 10
    assert price == 100


def test_classify_ipo_entry() -> None:
    entry = {
        "title": "Kutheli Bukhari Small Hydropower Limited IPO opens from 2026-03-20 to 2026-03-24",
        "details": "General public issue. Minimum apply 10 units. Maximum apply 500 units. Price per unit Rs 100",
        "announcement_date": "2026-03-15",
        "url": "https://merolagani.com/NewsDetail.aspx?newsID=99999",
        "source": "test_source",
    }

    classified = classify_ipo_entry(entry, "upcoming")

    assert classified["record_type"] == "upcoming"
    assert classified["issue_type"] == "ipo"
    assert classified["issue_status"] in {"upcoming", "open", "closed", "unknown"}
    assert classified["min_quantity"] == 10
    assert classified["max_quantity"] == 500
    assert classified["price_per_unit"] == 100


def test_derive_issue_status_result() -> None:
    assert derive_issue_status("2026-03-20", "2026-03-24", "result") == "result"


def test_classify_ipo_entry_nepali_month_range_defaults_to_upcoming() -> None:
    entry = {
        "title": "Kalinchowk Hydropower Limited is going to issue its 6,84,750.00 units of IPO shares to the general public starting from 22nd - 25th Chaitra, 2082",
        "details": "Kalinchowk Hydropower Limited is going to issue its 6,84,750.00 units of IPO shares to the general public starting from 22nd - 25th Chaitra, 2082",
        "announcement_date": "Mar 26, 2026",
        "url": "https://merolagani.com/AnnouncementDetail.aspx?id=64893",
        "source": "merolagani_upcoming",
    }

    classified = classify_ipo_entry(entry, "issue")

    assert classified["issue_status"] == "upcoming"
    assert classified["issue_open_date"] is None
    assert classified["issue_close_date"] is None
