# Scrappy

Scrappy is a Py project that collects NEPSE  data in a very Ml feeding format which is  made for my another project:

## What it collects

- Market snapshots to CSV:
	- `data/nepse/market_summary_timeseries.csv`
	- `data/nepse/today_price_timeseries.csv`
- Ipo feed to the JSON:
	- `data/ipo/ipo_feed.json`

## sources

- NEPSE(primary) [https://www.nepalstock.com/]
- Merolagani IPO pages (upcoming + result) [https://merolagani.com/] (primary for IPO feed)
- ShareSansar live table (fallback only for sumamry) [https://www.sharesansar.com/] (fallback)



## setup

```bash
python -m pip install -e .[dev]
```

Run jobs:

```bash
# market only
python -m scraper.cli market

# ipo only
python -m scraper.cli ipo

# for botth
python -m scraper.cli all
```

## Dev workflow

Install and enable git hooks for pre-commit checks:

```bash
pre-commit install
pre-commit run --all-files
```

Manualy quality checks:

```bash
ruff check .
black --check .
pytest -q tests
```
