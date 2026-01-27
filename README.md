# Moneycontrol Forum Sentiment Pipeline

This repo contains a small, configurable pipeline to scrape posts from Moneycontrol forum threads and score their sentiment for downstream strategies.

## What it does
- Crawls each thread URL page-by-page (using `rel=next`/`Next` buttons).
- Extracts post text plus optional metadata (post id, author, timestamp when visible).
- Scores sentiment with VADER (or a lightweight offline fallback).
- Writes post-level data to CSV and aggregate sentiment per thread to JSON.

## Quickstart
**Note:** If you're using PowerShell on Windows, replace backslashes `\` with backticks `` ` `` for line continuation, or run commands on a single line.

```bash
# 1) Install dependencies (ideally inside a venv)
python3 -m pip install -r requirements.txt

# 2) Run the pipeline
python3 -m moneycontrol_pipeline.pipeline \
  --urls https://mmb.moneycontrol.com/forum-topics/stocks/reliance-322.html \
  --max-pages 3 \
  --backend selenium --headless \
  --scroll-max 8 --scroll-limit 30 --scroll-pause 1.0 \
  --posts-out data/posts.csv \
  --summary-out data/summary.json
```

## URL Input Options
You can provide URLs in multiple ways:

### Single URL
```bash
python3 -m moneycontrol_pipeline.pipeline \
  --urls https://mmb.moneycontrol.com/forum-topics/stocks/reliance-322.html \
  --backend selenium --headless \
  --posts-out data/posts.csv \
  --summary-out data/summary.json
```


### Multiple URLs from CSV file
Use `--urls-csv` to read URLs from a CSV file (e.g., `final.csv`):

**Bash/Unix:**
```bash
python3 -m moneycontrol_pipeline.pipeline \
  --urls-csv data/final.csv \
  --csv-column forum_topics_url \
  --backend selenium --headless \
  --posts-out data/posts.csv \
  --summary-out data/summary.json
```

**PowerShell:**
```powershell
python3 -m moneycontrol_pipeline.pipeline `
  --urls-csv data/final.csv `
  --csv-column forum_topics_url `
  --backend selenium --headless `
  --posts-out data/posts.csv `
  --summary-out data/summary.json
```

Or as a single line in PowerShell:
```powershell
python3 -m moneycontrol_pipeline.pipeline --urls-csv data/final.csv --csv-column forum_topics_url --backend selenium --headless --posts-out data/posts.csv --summary-out data/summary.json
```

The `--csv-column` defaults to `forum_topics_url` which matches the structure of `final.csv`. You can specify a different column name if needed.

## Backends:
- `--backend selenium` when the forum content is rendered client-side. Headless Chrome via `--headless`; adjust scroll bounds with `--scroll-max`, `--scroll-limit`, `--scroll-pause`.
- `--backend api` to hit the Moneycontrol `mcapi/v2/mmb/get-messages` endpoint directly. This is fastest/most complete for historical data. Example for full history (beware of size: hundreds of thousands of posts):
  ```bash
  python3 -m moneycontrol_pipeline.pipeline \
    --backend api \
    --urls https://mmb.moneycontrol.com/forum-topics/stocks/reliance-322.html \
    --api-limit-count 200 \  # batch size per request
    --max-messages 0 \       # 0 = fetch everything
    --posts-out data/posts.csv \
    --summary-out data/summary.json
  ```

## Outputs
- `data/posts.csv`: one row per post with text, author (if found), page URL, and sentiment scores/label.
- `data/summary.json`: per-thread aggregates (`avg_compound`, positive/negative ratios, post counts).

## Adjusting scraping logic
Moneycontrol sometimes tweaks forum HTML. The scraper uses flexible selectors (`cmt`, `comment`, `post`, `data-post-id`, etc.) and a fallback that keeps sizeable blocks. If you notice empty outputs or missed posts, update `MoneycontrolScraper._find_post_elements`/`_find_first_text` in `moneycontrol_pipeline/scraper.py` with the latest classes/ids from the live page.

## Notes
- The sandbox here has no outbound network, so I could not live-test against the sample Reliance thread. The code relies on typical Moneycontrol forum patterns and should be tweaked if their markup differs.
- Sentiment defaults to VADER; if it is not installed the code falls back to a tiny lexicon, so keep VADER for higher fidelity.
# moneyControl_forum_scraper
