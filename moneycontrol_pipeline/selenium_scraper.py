import time
from typing import Dict, Iterable, List, Optional

import bs4
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .scraper import MoneycontrolScraper, Post, clean_text


POST_TEXT_SELECTOR = "div.postItem_text_paragraph__3XhZQ"
POST_HEADING_SELECTOR = "div.postItem_heading__2odZU"


class SeleniumMoneycontrolScraper(MoneycontrolScraper):
    """
    Selenium-backed scraper for Moneycontrol forum threads. This targets the
    observed React-rendered DOM where post headings and bodies live under the
    provided selectors.
    """

    def __init__(
        self,
        max_pages: int = 3,
        sleep_seconds: float = 1.2,
        timeout: int = 25,
        headless: bool = True,
        wait_selector: str = POST_TEXT_SELECTOR,
        scroll_max: int = 6,
        scroll_limit: int = 20,
        scroll_pause: float = 1.0,
    ) -> None:
        super().__init__(max_pages=max_pages, sleep_seconds=sleep_seconds, timeout=timeout)
        options = Options()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        self.driver = webdriver.Chrome(options=options)
        self.wait_selector = wait_selector
        self.scroll_max = scroll_max
        self.scroll_limit = scroll_limit
        self.scroll_pause = scroll_pause

    def fetch_pages(self, start_url: str) -> Iterable[Dict[str, str]]:
        current_url = start_url
        for _ in range(self.max_pages):
            self.driver.get(current_url)
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.wait_selector))
            )
            self._scroll_to_load_more()
            html = self.driver.page_source
            yield {"page_url": current_url, "source_url": start_url, "html": html}
            next_url = self._find_next_page(html, current_url)
            if not next_url:
                break
            current_url = next_url
            time.sleep(self.sleep_seconds)

    def parse_posts(self, html: str, page_url: str, source_url: str) -> List[Post]:
        soup = bs4.BeautifulSoup(html, "html.parser")
        text_nodes = soup.select(POST_TEXT_SELECTOR)
        heading_nodes = soup.select(POST_HEADING_SELECTOR)

        posts: List[Post] = []
        count = max(len(text_nodes), len(heading_nodes))
        for idx in range(count):
            heading = (
                clean_text(heading_nodes[idx].get_text(" ", strip=True))
                if idx < len(heading_nodes)
                else None
            )
            body = (
                clean_text(text_nodes[idx].get_text(" ", strip=True))
                if idx < len(text_nodes)
                else ""
            )
            if not heading and not body:
                continue

            posts.append(
                Post(
                    source_url=source_url,
                    page_url=page_url,
                    post_id=None,
                    author=None,
                    posted_at=None,
                    heading=heading,
                    text=body,
                )
            )
        return posts

    def _find_next_page(self, html: str, current_url: str) -> Optional[str]:
        # Reuse parent pagination logic.
        return super()._find_next_page(html, current_url)

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass

    def _scroll_to_load_more(self) -> None:
        """
        Scrolls to the bottom repeatedly to trigger lazy-loaded posts.
        Stops after scroll_max attempts without new content or height change.
        """
        attempts = 0
        loops = 0
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        last_count = len(self.driver.find_elements(By.CSS_SELECTOR, POST_TEXT_SELECTOR))

        while attempts < self.scroll_max and loops < self.scroll_limit:
            loops += 1
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.scroll_pause)
            try:
                WebDriverWait(self.driver, self.timeout).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, POST_TEXT_SELECTOR)) > last_count
                )
                last_count = len(self.driver.find_elements(By.CSS_SELECTOR, POST_TEXT_SELECTOR))
                attempts = 0
            except Exception:
                attempts += 1

            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                attempts += 1
            else:
                last_height = new_height
