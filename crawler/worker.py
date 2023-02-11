from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
from crawler.frontier import Frontier

class Worker(Thread):
    def __init__(self, worker_id, config, frontier: Frontier):
        self.logger = get_logger(f"Worker-{worker_id}", f"Worker{worker_id}")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            if not scraper.is_valid(tbd_url):
                self.frontier.add_filtered_url((tbd_url, "filtered out by text matching (already inserted in the queue)"))
                self.frontier.mark_url_complete(tbd_url)
                continue

            if scraper.without_parameter(tbd_url):
                if self.frontier.check_seen_url(tbd_url):
                    self.frontier.add_filtered_url((tbd_url, "filtered out already seen url (already inserted in the queue)"))
                    self.frontier.mark_url_complete(tbd_url)
                    continue

            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")

        
            is_valid, scraped_urls, filtered_urls, word_list = scraper.scraper(tbd_url, resp, self.logger)

            if is_valid:
                self.frontier.record_url(tbd_url)

            self.frontier.extract_info(tbd_url, word_list)
            for filtered_url in filtered_urls:
                self.frontier.add_filtered_url(filtered_url)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)

            time.sleep(self.config.time_delay)
