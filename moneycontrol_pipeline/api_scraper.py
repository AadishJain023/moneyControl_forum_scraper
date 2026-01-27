import re
from typing import List, Optional

import requests

from .scraper import Post, clean_text


class ApiMoneycontrolScraper:
    """
    Scrape Moneycontrol forum messages via the public mcapi/v2/mmb/get-messages endpoint.
    This is faster and more complete than DOM scraping, and returns metadata like user,
    timestamps, thread URLs, and message ids.
    """

    BASE_URL = "https://api.moneycontrol.com/mcapi/v2/mmb/get-messages/"

    def __init__(self, limit_count: int = 100, max_messages: int = 0, timeout: int = 25) -> None:
        self.limit_count = limit_count
        self.max_messages = max_messages
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def fetch_posts(self, start_url: str) -> List[Post]:
        section_id = parse_section_id(start_url)
        offset = 0
        posts: List[Post] = []

        while True:
            params = {
                "section": "topic",
                "sectionId": section_id,
                "limitStart": offset,
                "limitCount": self.limit_count,
                "msgIdReference": "",
            }
            resp = self.session.get(self.BASE_URL, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            batch = data.get("list", [])
            if not batch:
                break

            for msg in batch:
                heading = clean_text(msg.get("heading") or "")
                text = clean_text(msg.get("message") or "")
                if not heading and not text:
                    continue
                posts.append(
                    Post(
                        source_url=start_url,
                        page_url=msg.get("urlThread") or start_url,
                        post_id=str(msg.get("msg_id") or ""),
                        author=msg.get("user_nick_name") or msg.get("uidNickname"),
                        posted_at=msg.get("ent_date") or msg.get("repost_date"),
                        heading=heading or None,
                        text=text,
                    )
                )
                if self.max_messages and len(posts) >= self.max_messages:
                    return posts

            offset += self.limit_count
            if len(batch) < self.limit_count:
                break

        return posts


def parse_section_id(url: str) -> int:
    match = re.search(r"-([0-9]+)(?:\.html)?", url)
    if not match:
        raise ValueError(f"Could not parse sectionId from URL: {url}")
    return int(match.group(1))
