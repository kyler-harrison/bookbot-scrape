import requests
import nltk
import urllib
import time
import os
import traceback
import boto3
import enchant
import json
from multiprocessing import Process
from multiprocessing import Manager
from bs4 import BeautifulSoup as bs
from utils import *
import timeit


# globals
enchant_dict = enchant.Dict("en_US")
stop_words_ls = nltk.corpus.stopwords.words("english")
additional_sw = [".", "'", "-"]  # catching the chars that are allowed 
for sw in additional_sw: stop_words_ls.append(sw)
stop_words = set(stop_words_ls)  
lemmatizer = nltk.stem.WordNetLemmatizer()


# desc.: return list of urls from the url file
def load_urls(url_path):
    url_file = open(url_path, "r")
    urls = [line[:-1] for line in url_file.readlines()]
    url_file.close()
    return urls


# desc.: given text string, update valid_dict and invalid_dict with {"word": count}, valid/invalid dicts refer to dictionaries of each title 
def word_breaker(text, valid_dict, invalid_dict):
    word_ls = nltk.word_tokenize(text)
    for word in word_ls:
        word = word.lower()  # all words will be lowercased and lemmatized
        # not stopword, only contains valid characters
        if word not in stop_words and valid_word(word):
            # update total word counter for the title, used for calculating frequency of each word
            try:
                valid_dict["TOTAL_WORD_COUNT"] += 1
            except KeyError:
                valid_dict["TOTAL_WORD_COUNT"] = 1

            word_lem = lemmatizer.lemmatize(word)
            check = enchant_dict.check(word_lem)

            # case matters on proper nouns, this is kinda hacky, but whatever
            if not check:
                check = enchant_dict.check(word_lem.capitalize())

            if not check:
                try:
                    invalid_dict[word_lem] += 1
                except KeyError:
                    invalid_dict[word_lem] = 1
            else:
                try:
                    valid_dict[word_lem] += 1
                except KeyError:
                    valid_dict[word_lem] = 1


# desc.: given soup object, update valid/invalid dictionaries using the review text
def review_scrape(soup, valid_dict, invalid_dict):
    try:
        review_containers = soup.find_all("div", {"class": "reviewText stacked"})
        reviews = []
        for review_container in review_containers:
            review_sub_containers = review_container.find("span").find_all("span")
            # handle the ...more thing
            if len(review_sub_containers) == 2:
                word_breaker(review_sub_containers[1].text, valid_dict, invalid_dict)
            else:
                word_breaker(review_sub_containers[0].text, valid_dict, invalid_dict)
        return True
    except:
        return False


def book_scrape(url, metadata_dict, progress_dict, img_dir_path, img_bucket_name, db_client, db_table_name, s3_resource, valid_base_dir, master_dict):
    start_time = time.time()
    db_dict = {}  # dict for the database entry, written to aws if scrape successful 
    this_valid = {}  # valid words for this title, ex. {"word": 27}
    this_invalid = {}  # invalid words for this title, ex. {"asdfsdhewq": 1}
    metadata_str = ""  # string to add info for the mp metadata_dict, added to metadata dict as title_key: metadata_str if scrape successful

    # get the main page
    agent_headers = gen_agent_headers()
    data = requests.get(url, agent_headers)
    if data == None:
        message = f"ERROR: did not get {url}"
        time_elapsed = time.time() - start_time
        progress_dict[url] = message
        return 
    soup = bs(data.text, "html.parser")

    # title
    og_title = soup.find("h1", {"id": "bookTitle"}).text.strip()
    title = valid_chars(og_title, alpha_num=True).strip()
    if not title or title.isspace():  # title is None or just white spaces
        message = f"ERROR: no valid title {url}"
        time_elapsed = time.time() - start_time
        progress_dict[url] = message
        return
    db_dict["title"] = {"S": title}
    db_dict.update({"unfiltered_title": {"S": og_title}})
    metadata_str += title + ","
    word_breaker(title, this_valid, this_invalid)  
    this_valid["TITLE_KEY"] = title  # key needed for master dict update later

    # author(s)
    authors = soup.find_all("a", {"class": "authorName"})
    authors_text = list(set([valid_chars(author_item.text).strip() for author_item in authors]))
    if len(authors_text) == 0:
        message = "ERROR: no authors"
        time_elapsed = time.time() - start_time
        progress_dict[url] = message
        return
    for auth_txt in authors_text:
        if not auth_txt:
            message = "ERROR: no valid authors"
            time_elapsed = time.time() - start_time
            progress_dict[url] = message
            return
    db_dict.update({"authors": {"SS": authors_text}})
    metadata_str += "&".join(authors_text) + ","
    for author_text in authors_text: word_breaker(author_text, this_valid, this_invalid)
    
    # get genres 
    genre_containers = soup.find_all("div", {"class": "elementList"})
    genre_links = [genre_container.find("a", {"class": "actionLinkLite bookPageGenreLink"}) for genre_container in genre_containers]
    genres = list(set([valid_chars(genre_link.text) for genre_link in genre_links if genre_link != None]))
    genres_lower = [genre_str.lower() for genre_str in genres]
    if "nonfiction" in genres_lower or "non-fiction" in genres_lower or "non-fic" in genres_lower or "nonfic" in genres_lower:
        non_fic = True
    else:
        non_fic = False
    db_dict.update({"genres": {"SS": genres}})
    metadata_str += str(non_fic) + ","
    metadata_str += "&".join(genres) + ","
    for genre in genres: word_breaker(genre, this_valid, this_invalid)

    # description
    description_container = soup.find("div", {"id": "description"})
    description_subs = description_container.find_all("span")
    # handle the "...more" duplicate description thing
    if len(description_subs) > 1:
        description_sub = description_subs[1]
    else:
        description_sub = description_subs[0]
    book_description = description_sub.text
    db_dict.update({"description": {"S": book_description}})
    desc_sentences = breakdown_sentences(book_description) 
    valid_desc_sentences = [valid_chars(desc_sent) for desc_sent in desc_sentences]
    metadata_str += " ".join(valid_desc_sentences) + ","  # TODO is this neccessary anymore? anything else potentially irrelevant in metadata?
    word_breaker(book_description, this_valid, this_invalid)
    
    # get book details container (for finding the next few items)
    details = soup.find("div", {"id": "details"})

    # get number of pages
    num_pages_text = details.find("span", {"itemprop": "numberOfPages"}).text
    num_pages = ""
    for num_char in num_pages_text:
        # this works if the format is always like "paperback, 247 pages" (the first numbers in string are page numbers)
        try:  
            int(num_char)
            num_pages += num_char
        except ValueError:
            pass
    if num_pages == "":  # just in case
        num_pages = "None"
    metadata_str += num_pages + ","

    # publisher info, just one messy string of stuff (this may or may not work for everybody)
    publisher_statement_containers = details.find_all("div", {"class": "row"})
    publisher_statements = [item.text.strip() for item in publisher_statement_containers]
    publisher_statement = ""
    for i, pub in enumerate(publisher_statements):
        pub_ls = nltk.word_tokenize(pub)
        if i < 1:
            publisher_statement += valid_chars(" ".join(pub_ls))
        else:
            publisher_statement += " " + valid_chars(" ".join(pub_ls))
    metadata_str += publisher_statement.strip() + ","
    word_breaker(publisher_statement, this_valid, this_invalid)

    # get characters
    detail_links = details.find_all("a")
    # filter out the character links (I think this should be the same for all)
    characters = [valid_chars(detail_link.text) for detail_link in detail_links if str(detail_link)[0:21] == '<a href="/characters/']
    characters_str = "&".join(characters)
    metadata_str += characters_str + ","
    for character in characters: word_breaker(character, this_valid, this_invalid)

    # get setting
    setting_info = [detail_link.text for detail_link in detail_links if str(detail_link)[0:17] == '<a href="/places/']
    setting_info_ls = [valid_chars(set_stat) for set_stat in setting_info]
    setting_info_str = "&".join(setting_info_ls)
    metadata_str += setting_info_str + ","
    for setterboy in setting_info: word_breaker(setterboy, this_valid, this_invalid)

    # get other books by this author
    more_book_containers = soup.find_all("div", {"class": "js-tooltipTrigger tooltipTrigger"})
    more_book_titles = [valid_chars(container.find("a").find("img")["title"]) for container in more_book_containers]
    more_book_titles_str = "&".join(more_book_titles)
    metadata_str += more_book_titles_str + ","
    for more_book in more_book_titles: word_breaker(more_book, this_valid, this_invalid)

    # get related titles
    carousel_container = soup.find("div", {"class": "carouselRow"})
    carousel_li_elems = carousel_container.find("ul").find_all("li")
    related_titles_ls = [valid_chars(li_elem.find("a").find("img")["alt"]) for li_elem in carousel_li_elems]
    related_titles_str = "&".join(related_titles_ls)
    metadata_str += related_titles_str 
    for related_title in related_titles_ls: word_breaker(related_title, this_valid, this_invalid)
    
    # all things reviews
    first_page_review_status = review_scrape(soup, this_valid, this_invalid)  # write the first review page

    if first_page_review_status == False:
        message = "ERROR: first page review error"
        progress_dict[title] = message
        review_file.close()
        os.remove(review_path)  # don't need if error
        return

    # get the other review pages and write them
    review_links = soup.find("div", {"id": "reviews"}).find_all("a")
    review_request_urls = []
    if len(review_links) > 1:
        for link in review_links:
            try:
                link_onclick = link["onclick"]
                if link["href"] == "#" and link_onclick[0:8] == "new Ajax":
                    call = ("https://goodreads.com" + link_onclick[18:-16]).split('{')
                    call[0] = call[0][:-3]  # ending fix
                    call[1] = '{' + call[1]  # starting fix
                    uri_key_idx = call[1].find("encodeURIComponent") + 20  # finds encodeURIComponent and then adds to the start of the key
                    uri_key = call[1][uri_key_idx:-3]  # the unique uri key for each link (changes each time)
                    review_request_urls.append((call[0], uri_key))
            except KeyError:
                pass
        
        # for each next review page, request, scrape, write to file
        for rev_info in review_request_urls:
            auth_token = "authenticity_token=" + urllib.parse.quote(rev_info[1], safe="~()*!.\'")
            new_headers = gen_agent_headers()
            rev_data = requests.get(rev_info[0], params={"asynchronous": True, "evalScripts": True, "method": "get", "parameters": auth_token}, headers=new_headers)
            if rev_data == None:
                message = "ERROR: no data returned from review get request"
                time_elapsed = time.time() - start_time
                review_file.close()
                os.remove(review_path)
                progress_dict[url] = message
                return 

            actual_data = rev_data.text[27:-3]
            encoded_data = actual_data.encode("utf-8")
            decoded_data = encoded_data.decode("unicode-escape")
            rev_soup = bs(decoded_data, "html.parser")
            other_review_status = review_scrape(rev_soup, this_valid, this_invalid)
            if other_review_status == False:
                message = "ERROR: other review page error"
                progress_dict[title] = message
                review_file.close()
                os.remove(review_path)  # don't need if error
                return

    # download cover image locally
    cover_img_url = soup.find("img", {"id": "coverImage"})["src"]
    img_valid = True  # bool if image download/upload properly 

    # handle file extension stuff
    possible_ext = cover_img_url[-3:].lower()
    possible_ext_jpeg = cover_img_url[-4:].lower()
    if possible_ext == "jpg" or possible_ext == "png" or possible_ext_jpeg == "jpeg":
        cover_img_ext = possible_ext
    else:
        img_valid = False  

    # get and download image
    img_path = img_dir_path + "/" + title + "." + cover_img_ext
    img_headers = gen_agent_headers()
    try:
        if img_valid:
            with requests.get(cover_img_url, agent_headers, stream=True) as r:
                with open(img_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
    except:
        img_valid = False

    # upload cover image and remove local, set db img reference
    if img_valid:
        img_upload_success = upload_file_s3(s3_resource, img_path, cover_img_ext, title, img_bucket_name)
        if not img_upload_success:
            message = "ERROR: cover image failed to upload to s3"
            os.remove(img_path)
            progress_dict[title] = message
            return
        # since title key is alphanumeric, shouldn't have any issues here
        aws_img_ref = "https://{}.s3.amazonaws.com/{}".format(img_bucket_name, title + "." + cover_img_ext)  
    else:
        # TODO upload default image and name it this (hopefully there aren't any books with this name - is joke)
        aws_img_ref = "https://{}.s3.amazonaws.com/defaultNoImgAvailable".format(img_bucket_name)  
    os.remove(img_path)

    # write the db dict to the dyanmo db table
    db_dict.update({"cover_img_bucket_name": {"S": img_bucket_name}})
    db_dict.update({"cover_img_ref": {"S": aws_img_ref}})

    try:    
        db_client.put_item(TableName=db_table_name, Item=db_dict)
    except:
        message = "ERROR: failed to put data into dynamo"
        progress_dict[title] = message
        return 

    # add any invalid words that are potnetially valid
    #fixed = []
    valid_thresh = 5
    for invalid_word, count in this_invalid.items():
        if valid_word(invalid_word, alpha=True):
            if count > valid_thresh:
                #fixed.append((invalid_word, count))
                this_valid[invalid_word] = count  # I don't think a try/except is necessary here
                this_valid["TOTAL_WORD_COUNT"] += 1  # this shouldn't throw a key error

    # write the dictionary of this file to json
    valid_path = "{}/{}.json".format(valid_base_dir, title)
    with open(valid_path, "w") as vf:
        json.dump(this_valid, vf)

    # successful page 
    progress_dict[title] = "SUCCESS"

    # add this title to the shared dictionary
    metadata_dict[title] = metadata_str

    time_elapsed = time.time() - start_time
    print("\n{} done in {} seconds".format(title, time_elapsed))


def main():
    # load and batch urls TODO get scrape for the links file
    urls = ["https://www.goodreads.com/book/show/52578297-the-midnight-library?from_choice=true"]
    batch_size = 10
    batched_urls = batch_urls(batch_size, urls)

    # aws vars (on this machine credentials should be all setup, TODO aws cli config on cloud machine)
    db_client = boto3.client("dynamodb", region_name="us-east-1")
    db_table_name = "bookOracleData"
    s3_resource = boto3.resource("s3")
    img_dir_path = "data/cover_images"
    img_bucket_name = "bookoraclecoverimages"

    # paths and whatnot NOTE make sure metadata and progress files deleted if starting over
    metadata_path = "data/metadata/metadata.csv"
    metadata_mode = "a"
    progress_path = "data/metadata/progress.csv"
    progress_mode = "a"
    master_dict_path = "data/master_dict/master_dict.json"
    master_dict_mode = "w"
    valid_base_dir = "data/indiv_dicts"  # valid word jsons

    # TODO here, need to go through the below loop and make sure it fits with whiteboard, bring over functions for writing, scrape function done
    # shared dictionary to hold the all words from everything
    master_manager = Manager()
    master_dict = master_manager.dict()

    # go one batch (determined above) at a time
    for url_batch in batched_urls:
        # special process manager dictionaries for this batch
        metadata_manager = Manager()
        progress_manager = Manager()
        metadata_dict = metadata_manager.dict()
        progress_dict = progress_manager.dict()
        
        # multi-processing for all the urls
        processes = []
        #temp_valid_paths = []
        #temp_invalid_paths = []  # TODO rm this in function, don't really need to write this file
        for i, url in enumerate(url_batch):
            #valid_path = "data/indiv_dicts/valid_{}.json".format(str(i))
            #invalid_path = "data/indiv_dicts/invalid_{}.json".format(str(i))
            #temp_valid_paths.append(valid_path)
            #temp_invalid_paths.append(invalid_path)
            process = Process(target=book_scrape, args=(url, metadata_dict, progress_dict, img_dir_path, img_bucket_name, db_client, db_table_name, s3_resource, valid_base_dir, master_dict))
            processes.append(process)
            process.start()
        
        # back in sync
        for p in processes:
            p.join()

        # update the master dict with the json dicts just collected
        master_dict = dict(master_dict)  # think about efficiency later
        progress_dict = dict(progress_dict)
        for title_key, title_status in progress_dict.items():
            if title_status == "SUCCESS":
                path = "{}/{}.json".format(valid_base_dir, title_key)
                update_master_dict(path, master_dict)
        master_dict = master_manager.dict(master_dict)  # again with the efficiency

        # write metadata (only successful titles should be here)
        write_metadata(metadata_dict, metadata_path, metadata_mode)

        # write progress (all titles should be here, error messages for failures)
        main_elapsed = time.time() - main_start
        # TODO pass in progress dict and make proper updates above, do error handling
        progress_dict["MAIN TIME (S)"] = str(main_elapsed)
        write_progress(progress_dict, progress_path, progress_mode)

        print("{} seconds for {} titles".format(main_elapsed, len(urls)))
    
    master_dict = dict(master_dict)  # make the mp dict into a nromal python dictionary
    write_dict_to_json(master_dict, master_dict_path)


if __name__ == "__main__":
    main()


