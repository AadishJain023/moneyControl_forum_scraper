import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from .scraper import MoneycontrolScraper, Post
from .selenium_scraper import SeleniumMoneycontrolScraper
from .api_scraper import ApiMoneycontrolScraper
from .sentiment import SentimentAnalyzer


def run_pipeline(
    urls: Sequence[str],
    max_pages: int = 3,
    sleep_seconds: float = 1.2,
    posts_out: Optional[str] = None,
    summary_out: Optional[str] = None,
    backend: str = "requests",
    headless: bool = True,
    scroll_max: int = 6,
    scroll_limit: int = 20,
    scroll_pause: float = 1.0,
    api_limit_count: int = 100,
    max_messages: int = 0,
) -> Dict[str, object]:
    if backend == "selenium":
        scraper = SeleniumMoneycontrolScraper(
            max_pages=max_pages,
            sleep_seconds=sleep_seconds,
            headless=headless,
            scroll_max=scroll_max,
            scroll_limit=scroll_limit,
            scroll_pause=scroll_pause,
        )
    elif backend == "api":
        scraper = ApiMoneycontrolScraper(
            limit_count=api_limit_count, max_messages=max_messages, timeout=25
        )
    else:
        scraper = MoneycontrolScraper(max_pages=max_pages, sleep_seconds=sleep_seconds)
    analyzer = SentimentAnalyzer()

    all_posts: List[Dict[str, object]] = []
    failed_urls: List[str] = []
    try:
        if backend == "api":
            for idx, url in enumerate(urls, 1):
                print(f"[{idx}/{len(urls)}] Processing: {url}")
                try:
                    posts = scraper.fetch_posts(url)
                    _append_posts(all_posts, posts, analyzer)
                    print(f"  ✓ Found {len(posts)} posts")
                except Exception as e:
                    print(f"  ✗ Error: {str(e)[:100]}")
                    failed_urls.append(url)
        else:
            for idx, url in enumerate(urls, 1):
                print(f"[{idx}/{len(urls)}] Processing: {url}")
                try:
                    page_count = 0
                    for page in scraper.fetch_pages(url):
                        page_count += 1
                        posts = scraper.parse_posts(
                            html=page["html"], page_url=page["page_url"], source_url=page["source_url"]
                        )
                        _append_posts(all_posts, posts, analyzer)
                    print(f"  ✓ Scraped {page_count} pages, total posts so far: {len(all_posts)}")
                except Exception as e:
                    print(f"  ✗ Error: {str(e)[:100]}")
                    failed_urls.append(url)
    finally:
        if hasattr(scraper, "close"):
            try:
                scraper.close()
            except Exception:
                pass

    summary = aggregate(all_posts)

    if posts_out:
        write_csv(posts_out, all_posts)
    if summary_out:
        write_json(summary_out, summary)

    return {"posts": all_posts, "summary": summary, "failed_urls": failed_urls}


def aggregate(posts: List[Dict[str, object]]) -> List[Dict[str, object]]:
    grouped: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for post in posts:
        grouped[post["source_url"]].append(post)

    summary: List[Dict[str, object]] = []
    for url, group in grouped.items():
        count = len(group)
        if not count:
            continue
        avg_compound = sum(float(p["sentiment_compound"]) for p in group) / count
        pos = sum(1 for p in group if p["sentiment_label"] == "positive")
        neg = sum(1 for p in group if p["sentiment_label"] == "negative")
        neutral = count - pos - neg
        summary.append(
            {
                "source_url": url,
                "posts": count,
                "avg_compound": avg_compound,
                "positive_ratio": pos / count,
                "negative_ratio": neg / count,
                "neutral_ratio": neutral / count,
            }
        )
    return summary


def _append_posts(container: List[Dict[str, object]], posts: List[Post], analyzer: SentimentAnalyzer):
    for post in posts:
        content = " ".join([part for part in [post.heading, post.text] if part])
        sent = analyzer.score(content or post.text)
        container.append(
            {
                "source_url": post.source_url,
                "page_url": post.page_url,
                "post_id": post.post_id,
                "author": post.author,
                "posted_at": post.posted_at,
                "heading": post.heading,
                "text": post.text,
                "sentiment_compound": sent["compound"],
                "sentiment_label": sent["label"],
                "sentiment_pos": sent.get("pos", 0.0),
                "sentiment_neg": sent.get("neg", 0.0),
                "sentiment_neu": sent.get("neu", 0.0),
            }
        )


def write_csv(path: str, rows: Iterable[Dict[str, object]]) -> None:
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    if not rows:
        path_obj.write_text("")
        return

    fieldnames = list(rows[0].keys())
    with path_obj.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: str, data: object) -> None:
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    path_obj.write_text(json.dumps(data, indent=2))


def load_urls(url_args: Sequence[str], urls_file: Optional[str], urls_csv: Optional[str] = None, csv_column: str = "forum_topics_url") -> List[str]:
    urls: List[str] = list(url_args)
    if urls_file:
        file_urls = [
            line.strip()
            for line in Path(urls_file).read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        urls.extend(file_urls)
        print(f"[INFO] Loaded {len(file_urls)} URLs from {urls_file}")
    if urls_csv:
        csv_urls = []
        try:
            with open(urls_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames and csv_column not in reader.fieldnames:
                    raise ValueError(f"Column '{csv_column}' not found in CSV. Available columns: {reader.fieldnames}")
                for row in reader:
                    url = row.get(csv_column, "").strip()
                    if url and not url.startswith("#"):
                        csv_urls.append(url)
        except Exception as e:
            raise ValueError(f"Error reading URLs from CSV '{urls_csv}': {e}")
        urls.extend(csv_urls)
        print(f"[INFO] Loaded {len(csv_urls)} URLs from CSV {urls_csv}")
    return urls


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape Moneycontrol forum threads and run sentiment scoring. "
            "Outputs a CSV of posts and a JSON summary ready for strategy ingestion."
        )
    )
    parser.add_argument("--urls", nargs="*", help="Thread URLs to scrape", default=[])
    parser.add_argument(
        "--urls-file",
        help="Optional newline-delimited file containing thread URLs",
    )
    parser.add_argument(
        "--urls-csv",
        help="Optional CSV file containing thread URLs (e.g., final.csv)",
    )
    parser.add_argument(
        "--csv-column",
        default="forum_topics_url",
        help="Column name in CSV file containing URLs (default: 'forum_topics_url')",
    )
    parser.add_argument("--max-pages", type=int, default=3, help="Maximum pages per thread")
    parser.add_argument("--sleep", type=float, default=1.2, help="Seconds to sleep between pages")
    parser.add_argument("--posts-out", default="data/posts.csv", help="CSV path for post-level data")
    parser.add_argument(
        "--summary-out",
        default="data/summary.json",
        help="JSON path for aggregate sentiment per thread",
    )
    parser.add_argument(
        "--backend",
        choices=["requests", "selenium", "api"],
        default="requests",
        help="Scraping backend; use selenium for dynamic pages.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=False,
        help="Run the browser in headless mode when backend=selenium.",
    )
    parser.add_argument(
        "--scroll-max",
        type=int,
        default=6,
        help="For selenium backend: max consecutive scroll attempts without new content before stopping.",
    )
    parser.add_argument(
        "--scroll-limit",
        type=int,
        default=20,
        help="For selenium backend: hard cap on total scroll loops.",
    )
    parser.add_argument(
        "--scroll-pause",
        type=float,
        default=1.0,
        help="For selenium backend: seconds to wait between scrolls.",
    )
    parser.add_argument(
        "--api-limit-count",
        type=int,
        default=100,
        help="For api backend: batch size per request (mcapi limitCount).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="For api backend: cap on total messages to fetch (0 = no cap).",
    )
    args = parser.parse_args(argv)
    
    # Debug: Print what we received
    print(f"[DEBUG] args.urls_csv = {args.urls_csv}")
    print(f"[DEBUG] args.csv_column = {args.csv_column}")

    urls = load_urls(args.urls, args.urls_file, args.urls_csv, args.csv_column)
    if not urls:
        raise SystemExit("Provide at least one thread URL via --urls, --urls-file, or --urls-csv.")
    
    print(f"[INFO] Total URLs to process: {len(urls)}")
    for i, url in enumerate(urls[:5], 1):  # Show first 5 URLs
        print(f"  {i}. {url}")
    if len(urls) > 5:
        print(f"  ... and {len(urls) - 5} more URLs")

    result = run_pipeline(
        urls=urls,
        max_pages=args.max_pages,
        sleep_seconds=args.sleep,
        posts_out=args.posts_out,
        summary_out=args.summary_out,
        backend=args.backend,
        headless=args.headless,
        scroll_max=args.scroll_max,
        scroll_limit=args.scroll_limit,
        scroll_pause=args.scroll_pause,
        api_limit_count=args.api_limit_count,
        max_messages=args.max_messages,
    )

    print(
        f"Scraped {len(result['posts'])} posts across {len(urls)} threads. "
        f"Summary written to {args.summary_out}, posts to {args.posts_out}."
    )
    if result['failed_urls']:
        print(f"[WARNING] Failed to process {len(result['failed_urls'])} URLs. Check logs above for details.")


if __name__ == "__main__":
    main()
