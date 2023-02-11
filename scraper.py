import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from utils.response import Response
import posixpath
from utils import *
import enchant


# Extract enough information and return a list of URLs
def scraper(url, resp, logger):
    status_code = resp.status
    error_code = resp.error

    if resp.error and "Spacetime Response" in resp.error:
        return False, [], [(url, f"space time error")], []

    if status_code != 200:
        if status_code >= 600:
            logger.info(error_code)
        return False, [], [(url, f"status code {status_code}")], []

    if resp.raw and "text/html" not in resp.raw.headers["content-type"]:
        content_type = resp.raw.headers["content-type"]
        return False, [], [(url, f"not text/html but {content_type}")], []
    elif resp.raw_response.content.startswith(b"%PDF"):
        # PDF
        return False, [], [(url, "pdf file")], []
    elif resp.raw_response.content.startswith(b"\xFF\xD8\xFF\xE0"):
        # JPG
        return False, [], [(url, "jpg file")], []
    # elif not is_valid(resp.url):
    #    return []
    
    # Return a list of URLs
    links = extract_next_links(url, resp, logger)
    links = unique(links)
    
    next_links = [link for link in links if is_valid(link)]
    filtered_links = [(link, "filtered by text matching") for link in links if not is_valid(link)]

    # Extract English word list
    word_list = html2text(resp)
    
    return True, next_links, filtered_links, word_list


def html2text(resp):
    # https://stackoverflow.com/questions/328356/extracting-text-from-html-file-using-python

    soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()    # rip it out
    # get text
    text = soup.get_text()
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    d = enchant.Dict("en_US")
    word_list = []
    for word in text.split():
        word = word.replace("\x00", "")
        word = word.lower()

        if word and d.check(word):
            if word not in stop_words:
                word_list.append(word)

    

    return word_list

def extract_next_links(url, resp: Response, logger):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    access_url = url
    actual_url = resp.url
    status_code = resp.status
    error_code = resp.error

    link_list = []

    soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
    for link in soup.find_all("a"):
        href = link.get('href')

        if href == "" or href is None:
            continue
            
        href = urljoin(url, href)
        parsed_href = urlparse(href)

        if not (bool(parsed_href.netloc) and bool(parsed_href.scheme)):
            continue
        
        """
        rel = link.get("rel")
        if rel:
            if "nofollow" in rel:
                logger.info(f"url with nofollow attribute: {url}")
                continue
        """

        if without_parameter(href):
            parsed_href = parsed_href._replace(query="", params="")
            
        url_candidate = parsed_href._replace(fragment="").geturl()
        url_candidate = url_candidate.strip()
        link_list.append(str(url_candidate))

    return link_list


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        if not check_file_url1(parsed) or not check_file_url2(parsed) or not check_file_url3(parsed):
            return False
        if not check_domain_path(parsed):
            return False
        if not check_calendar(parsed):
            return False
        if not check_less_information(parsed):
            return False
        if not check_less_info(parsed):
            return False
        if not check_dataset(parsed):
            return False
        if not check_trap(parsed):
            return False
        return True

    except TypeError:
        print ("TypeError for ", parsed)
        raise


def check_domain_path(parsed):
    allowed_domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]

    netloc = parsed.netloc.lower()
    if not netloc:
        return False
    path = parsed.path

    if any((netloc == d) or (netloc.endswith(f".{d}")) for d in allowed_domains):
        return True

    if netloc == "today.uci.edu":
        if path.startswith("/department/information_computer_sciences/"):
            return True
    
    return False

def check_file_url1(parsed):
    return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

def check_file_url2(parsed):
    deny_extensions = {'.' + e for e in IGNORED_EXTENSIONS}
    return not posixpath.splitext(parsed.path)[1].lower() in deny_extensions

def check_file_url3(parsed):
    url = parsed.geturl()
    return not any(url.endswith(f".{extension}") for extension in IGNORED_EXTENSIONS)

def check_calendar(parsed):
    # "http://calendar.ics.uci.edu/calendar.php" 404
    calendar_urls = "https://wics.ics.uci.edu/events/2021-10-13"
    calendar_urls = urlparse(calendar_urls)

    netloc = parsed.netloc.lower()
    path = parsed.path.lower()

    if netloc == calendar_urls.netloc.lower():
        if path.startswith("/events/"):
            if any(str(year) in path for year in range(2000, 2022)):
                return False
    
    return True

def check_less_information(parsed):
    no_information_urls = ["http://sli.ics.uci.edu/Classes/Classes?action=login"]
    if "action=login" in parsed.geturl():
        return False

    return True

def check_trap(parsed):
    domains = ["https://www.ics.uci.edu/alumni",
    "https://www.ics.uci.edu/community"]

    if is_subdomain(parsed, domains):
        return False

    return True

def check_less_info(parsed):
    domains = ["https://ngs.ics.uci.edu/author/",
     "https://ngs.ics.uci.edu/category/",
      "https://ngs.ics.uci.edu/tag/",
       "https://www.ics.uci.edu/~wjohnson/BIDA/",
        "https://www.ics.uci.edu/honors",
         "http://ics.uci.edu/honors",
          "https://www.ics.uci.edu/ugrad",
           "http://www.cert.ics.uci.edu/seminar",
           "http://www.cert.ics.uci.edu/EMWS09",] 

    if is_subdomain(parsed, domains):
        return False
    elif is_subdomain(parsed, ["https://gitlab.ics.uci.edu"]) and "-" in parsed.geturl(): 
        return False
    
    return True

def is_subdomain(parsed, domains):
    domains = [urlparse(domain)._replace(scheme="").geturl() for domain in domains]
    without_scheme = parsed._replace(scheme="").geturl()

    return any(without_scheme.startswith(domain) for domain in domains)



def without_parameter(url):
    domains = ["https://swiki.ics.uci.edu/doku.php",
     "http://www.ics.uci.edu/download/download.inc.php",
      "https://archive.ics.uci.edu/ml/datasets.php",
       "https://www.ics.uci.edu/honors",
        "https://www.ics.uci.edu/ugrad",
         "https://wiki.ics.uci.edu/doku.php",
          "https://grape.ics.uci.edu/wiki",
           "https://cbcl.ics.uci.edu/doku.php",
           "https://gitlab.ics.uci.edu"]
    parsed = urlparse(url)

    if is_subdomain(parsed, domains):
        return True

    return False

def check_dataset(parsed):
    domains = ["https://archive.ics.uci.edu/ml/machine-learning-databases"]

    if any(parsed.geturl().startswith(domain) for domain in domains):
        return False

    return True