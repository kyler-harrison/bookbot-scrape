from bs4 import BeautifulSoup as bs
from multiprocessing import Process
from multiprocessing import Manager
import requests
from my_fake_useragent import UserAgent
from utils import *
import time
import pandas as pd


# desc.: given path to the file that is base_url,page_count returns [(base_url0, page_count0), ..., (base_urlN, page_countN)]
def load_base_urls(base_url_path):
    df = pd.read_csv(base_url_path, names=["base_url", "page_count"])
    urls = df.base_url.to_list()
    page_counts = df.page_count.to_list()
    return [(url, page_count) for url, page_count in zip(urls, page_counts)]


# given first page of url and its max page number, generate list of urls (each is a page of the list - which contain links to books)
def get_all_page_urls(base_url, max_page_number):
    return [base_url] + [base_url + "?page=" + str(i) for i in range(2, max_page_number + 1)]


# given list page url and multiprocessing Manager dict, get book links and put them in the dict as base_url: [links...]
def get_book_urls(url, special_return_dict):
    start = time.time()
    agent_headers = gen_agent_headers()
    data = requests.get(url, agent_headers)
    soup = bs(data.text, "html.parser")

    # get the book title urls 
    title_tags = soup.find_all("a", {"class": "bookTitle"})
    title_links = ["https://www.goodreads.com" + title_tag["href"] for title_tag in title_tags]

    # add titles to mp dictionary
    special_return_dict[url] = title_links

    elapsed = time.time() - start
    print(f"{url} done in {elapsed} seconds\n")


# given the special multiprocessing dictionary of base_url: [links...], get unique set of links and write to given out_path
def write_return_dict(special_return_dict, out_path, out_mode):
    all_links = []
    for key, value_list in special_return_dict.items():
        for link in value_list:
            if link not in all_links:
                all_links.append(link)

    with open(out_path, out_mode) as f:
        for link in all_links:
            f.write(f"{link}\n")


def main():
    start = time.time()

    # load base urls
    base_url_path = "data/urls/base_urls.csv"
    base_urls = load_base_urls(base_url_path)

    # url creation based on base urls
    all_sep_urls = [get_all_page_urls(info_tuple[0], info_tuple[1]) for info_tuple in base_urls]

    # put all urls in one list
    all_urls = []
    for all_ls in all_sep_urls:
        for sub_url in all_ls:
            all_urls.append(sub_url)

    # batch urls for multiprocessing
    batch_size = 10
    batched_urls = batch_urls(batch_size, all_urls)

    # scrape one batch at a time, join and add to the mp dict as url: [title_urls], go on to next batch
    return_manager = Manager()
    return_dict = return_manager.dict()
    for url_batch in batched_urls:
        # track processes in this batch
        processes = []
        for url in url_batch:
            process = Process(target=get_book_urls, args=(url, return_dict))
            processes.append(process)
            process.start()

        for p in processes:
            p.join()

    # write all the urls (fun checks for uniques only)
    out_path = "data/urls/title_urls.txt"
    out_mode = "w"
    write_return_dict(return_dict, out_path, out_mode)

    elapsed = time.time() - start
    print(f"{len(batched_urls)} batches of {batch_size} scraped in {elapsed} seconds")


if __name__ == "__main__":
    main()


