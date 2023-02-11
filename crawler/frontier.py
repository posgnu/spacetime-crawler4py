import os
import shelve
from collections import Counter
from urllib.parse import urlparse
import time
from collections import defaultdict

from threading import Lock
import threading

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class Frontier(object):
    def __init__(self, config, restart):
        self.lock = Lock()
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.seen_filtered_url = set()
        self.max_page_length = 0
        self.max_page_url = ""
        self.max_len_page_file_name = "Logs/max_len_page.txt"
        self.counter = Counter()
        self.ics_domain = Counter()
        self.record_count = 0
        self.ics_domain_file_name = "Logs/ics_domain.txt"
        self.common_words_file_name ="Logs/common_words.txt"
        self.filter_url_file_name = "Logs/filtered_url.txt"
        self.timestamps = defaultdict(int)
        
        if restart:
            if os.path.exists(self.config.save_file + ".dat"):
                self.logger.info(f"Found save file {self.config.save_file}, deleting it.")
                self.save = shelve.open(self.config.save_file, flag="n")
            self.url_file = open("Logs/url_list.txt", "w")
            self.filtered_url = open(self.filter_url_file_name, "w")
            with open(self.max_len_page_file_name, "w") as f:
                f.write(f"dummy {self.max_page_length}")
        else:
            if not os.path.exists(self.config.save_file + ".dat"):
                self.logger.info(f"Did not find save file {self.config.save_file}, "f"starting from seed.")
                raise
                
            self.save = shelve.open(self.config.save_file)
            self.url_file = open("Logs/url_list.txt", "a")
            self.filtered_url = open(self.filter_url_file_name, "a")

            with open(self.common_words_file_name, "r") as f:
                lines = f.readlines()
                for line in lines:
                    word, count = line.split(", ")
                    count = int(count)
                    self.counter[word] = count
            
            self.logger.info("common word loaded")

            with open(self.ics_domain_file_name, "r") as f:
                lines = f.readlines()
                for line in lines:
                    domain, count = line.split(", ")
                    count = int(count)
                    self.ics_domain[domain] = count
            self.logger.info("ics domain loaded")
            
            with open(self.filter_url_file_name, "r") as f:
                lines = f.readlines()
                for line in lines:
                    domain = line.split(", ")[0]
                    self.seen_filtered_url.add(domain)
            self.logger.info("seen filtered url loaded")

            with open(self.max_len_page_file_name, "r") as f:
                _, self.max_page_length = f.read().split("\n")[-1].split()
                self.max_page_length = int(self.max_page_length)    
            
            self.logger.info("max len page loaded")
            
        
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)
        

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        if self.record_count % 5000 == 0:
            self.record_info()
        else:
            self.record_count += 1

        try:
            while True:
                self.logger.info(f"{threading.get_ident()} lock 1")
                self.lock.acquire()
                for idx, tbd_url in enumerate(self.to_be_downloaded):
                    parsed = urlparse(tbd_url)
                    netloc = parsed.netloc.lower()

                    past_timestamp = self.timestamps[netloc]
                    current_timestamp = time.time()
                    if (current_timestamp - past_timestamp) > self.config.time_delay:
                        self.timestamps[netloc] = current_timestamp
                        self.to_be_downloaded.pop(idx)
                    else:
                        continue

                    if self.is_subdomain(netloc, "ics.uci.edu"):
                        pass
                    elif self.is_subdomain(netloc, "cs.uci.edu"):
                        pass
                    elif self.is_subdomain(netloc, "informatics.uci.edu"):
                        pass
                    elif self.is_subdomain(netloc, "stat.uci.edu"):
                        pass
                    elif (netloc + parsed.path).startswith("today.uci.edu/department/information_computer_sciences"):
                        pass
                    else:
                        raise ValueError(f"Invalid url {tbd_url}")

                    self.logger.info(f"{threading.get_ident()} relase 1")
                    self.lock.release()
                    return tbd_url
                self.logger.info(f"{threading.get_ident()} relase 1")
                self.lock.release()
        except IndexError:
            self.logger.info(f"{threading.get_ident()} relase 1")
            self.lock.release()
            return None

    def is_subdomain(self, netloc, domain):
        return netloc == domain or netloc.endswith("."+ domain)
    
    def add_filtered_url(self, url_with_error: tuple):
        self.logger.info(f"{threading.get_ident()} lock 2")
        self.lock.acquire()
        url, error = url_with_error
        if url not in self.seen_filtered_url:
            self.filtered_url.write(f"{url}, {error}\n")
            self.filtered_url.flush()
            self.seen_filtered_url.add(url)
        self.logger.info(f"{threading.get_ident()} relase 2")
        self.lock.release()

    def check_seen_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)

        self.logger.info(f"{threading.get_ident()} lock 3")
        self.lock.acquire()
        
        if urlhash in self.save:
            self.lock.release()
            return True
    
        self.logger.info(f"{threading.get_ident()} relase 3")
        self.lock.release()

        return False

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)


        self.logger.info(f"{threading.get_ident()} lock 4")
        self.lock.acquire()
        
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded.append(url)

            parsed = urlparse(url)
            netloc = parsed.netloc
            if netloc == "ics.uci.edu" or netloc.endswith(".ics.uci.edu"):
                self.ics_domain[netloc] += 1
        self.logger.info(f"{threading.get_ident()} relase 4")
        self.lock.release()

        return

    def record_url(self, url):
        self.logger.info(f"{threading.get_ident()} lock 5")
        self.lock.acquire()
        self.url_file.write(f"{url}\n")
        self.url_file.flush()
        self.logger.info(f"{threading.get_ident()} relase 5")
        self.lock.release()

    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)

        self.logger.info(f"{threading.get_ident()} lock 6")
        self.lock.acquire()
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()
        self.logger.info(f"{threading.get_ident()} relase 6")
        self.lock.release()

        return 
    
    def extract_info(self, url, word_list):
        self.logger.info(f"{threading.get_ident()} lock 7")
        self.lock.acquire()

        page_len = len(word_list)

        if self.max_page_length < page_len:
            self.max_page_length = page_len
            self.max_page_url = url
            with open(self.max_len_page_file_name, "a") as f:
                f.write(f"{self.max_page_url} {self.max_page_length}\n")
            
        
        new_counter = Counter(word_list)
        self.counter = self.counter + new_counter
        self.logger.info(f"{threading.get_ident()} relase 7")
        self.lock.release()

    def record_info(self):
        # Record 50 common words
        with open(self.common_words_file_name, "w") as f:
            for (word, count) in self.counter.most_common(200):
                f.write(f"{word}, {count}\n")

        # Record subdomain of ics.uci.edu
        with open(self.ics_domain_file_name, "w") as f:
            for (domain, count) in sorted(self.ics_domain.items()):
                f.write(f"{domain}, {count}\n")
        

        

