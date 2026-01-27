import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import bs4
import requests


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
)


@dataclass
class Post:
    source_url: str
    page_url: str
    post_id: Optional[str]
    author: Optional[str]
    posted_at: Optional[str]
    heading: Optional[str]
    text: str


class MoneycontrolScraper:
    """Scrape Moneycontrol forum threads page by page."""

    def __init__(
        self,
        max_pages: int = 3,
        sleep_seconds: float = 1.2,
        timeout: int = 25,
    ) -> None:
        self.max_pages = max_pages
        self.sleep_seconds = sleep_seconds
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def fetch_pages(self, start_url: str) -> Iterable[Dict[str, str]]:
        """Yield dicts containing page URL and HTML for each page in the thread."""
        current_url = start_url
        for idx in range(self.max_pages):
            resp = self.session.get(current_url, timeout=self.timeout)
            resp.raise_for_status()
            html = resp.text
            yield {"page_url": current_url, "source_url": start_url, "html": html}
            next_url = self._find_next_page(html, current_url)
            if not next_url:
                break
            current_url = next_url
            time.sleep(self.sleep_seconds)

    def parse_posts(self, html: str, page_url: str, source_url: str) -> List[Post]:
        soup = bs4.BeautifulSoup(html, "html.parser")
        elements = self._find_post_elements(soup)

        posts: List[Post] = []
        seen_text = set()
        for el in elements:
            text = clean_text(" ".join(el.stripped_strings))
            if not text or text in seen_text:
                continue

            seen_text.add(text)
            post_id = el.get("id") or el.get("data-post-id") or el.get("data-msgid")
            author = self._find_first_text(el, ["author", "user", "name", "by"])
            posted_at = self._find_first_text(el, ["time", "date", "posted"])
            heading = self._find_first_text(el, ["heading", "title"])
            posts.append(
                Post(
                    source_url=source_url,
                    page_url=page_url,
                    post_id=post_id,
                    author=author,
                    posted_at=posted_at,
                    heading=heading,
                    text=text,
                )
            )
        return posts

    def _find_post_elements(self, soup: bs4.BeautifulSoup) -> List[bs4.element.Tag]:
        """Heuristically find post blocks on a forum page."""
        selectors = [
            "div[id*='cmt'], li[id*='cmt'], article[id*='cmt']",
            "div[class*='cmt'], li[class*='cmt'], article[class*='cmt']",
            "div[id*='comment'], li[id*='comment'], article[id*='comment']",
            "div[class*='comment'], li[class*='comment'], article[class*='comment']",
            "div[id*='post'], li[id*='post'], article[id*='post']",
            "div[class*='post'], li[class*='post'], article[class*='post']",
            "[data-post-id], [data-msgid]",
        ]

        for selector in selectors:
            matches = soup.select(selector)
            if matches:
                return matches

        # Fallback: pick sizeable <li>/<article>/<div> blocks
        candidates = []
        for el in soup.find_all(["article", "li", "div"]):
            text = clean_text(" ".join(el.stripped_strings))
            if text and len(text) > 80:
                candidates.append(el)
        return candidates

    def _find_first_text(self, el: bs4.element.Tag, keywords: List[str]) -> Optional[str]:
        """Search for child nodes whose class/id/title contains any keyword."""
        for child in el.find_all(True):
            attrs = " ".join(
                [
                    " ".join(v) if isinstance(v, (list, tuple)) else str(v)
                    for v in child.attrs.values()
                ]
            ).lower()
            if any(kw in attrs for kw in keywords):
                text = clean_text(" ".join(child.stripped_strings))
                if text:
                    return text
        return None

    def _find_next_page(self, html: str, current_url: str) -> Optional[str]:
        soup = bs4.BeautifulSoup(html, "html.parser")
        # rel="next"
        link = soup.find("a", rel=lambda val: val and "next" in val)
        if link and link.get("href"):
            return urljoin(current_url, link["href"])

        # anchors labeled "Next"
        for anchor in soup.find_all("a"):
            label = (anchor.get_text() or "").strip().lower()
            classes = " ".join(anchor.get("class", [])).lower()
            aria = (anchor.get("aria-label") or "").lower()
            if (
                "next" in label
                or "next" in classes
                or "next" in aria
                or label in {">", "›", "»"}
            ):
                href = anchor.get("href")
                if href:
                    return urljoin(current_url, href)
        return None


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text
