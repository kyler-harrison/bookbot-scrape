import json
import time
from utils import *


# NOTE it is assumed that the dictionaries will not have overlapping titles, which they shouldn't so long as you check for this each time 
# you add new urls in link_scrape.py (you have for the ~9200 collected on 1/29)
# desc.: if you have a previous master dict json, and you scrape some new data and save that master dict somewhere else, this
#        will update the previous master dict with the new master dict data and then write to out_json_path
#        smaller=0 means previous json is smaller than new, smaller=1 means opposite (efficiency)

# NOTE it is assumed that dictionaries will not have overlapping titles, which they shouldn't as link_scrape functions handle uniqueness
# desc.: merge two jsons of data, this is for either joining two jsons of the form {"word": {"title": 0.3}, ..., {"wordN"}: {"titleN": 0.2}} or
#        of the form {"title": {metadata}, ..., {"titleN": {metadataN}}}
def merge_jsons(previous_json_path, new_json_path, out_json_path, smaller=1):
    start_time = time.time()

    prev_dict = load_dict_from_json(previous_json_path)
    new_dict = load_dict_from_json(new_json_path)

    if smaller == 1:
        for word_key, title_freq_dict in new_dict.items():
            try:
                # existing words dict updates with the new {title: 0.1} 
                prev_dict[word_key].update(title_freq_dict)
            except KeyError:
                # if the word in the new dict doesn't exist, add it to the previous and put its dict in 
                prev_dict[word_key] = title_freq_dict
        write_dict_to_json(prev_dict, out_json_path)
    else:
        for word_key, title_freq_dict in prev_dict.items():
            try:
                # existing words dict updates with the new {title: 0.1} 
                new_dict[word_key].update(title_freq_dict)
            except KeyError:
                # if the word in the new dict doesn't exist, add it to the previous and put its dict in 
                new_dict[word_key] = title_freq_dict
        write_dict_to_json(new_dict, out_json_path)

    elapsed_time = time.time() - start_time
    print(f"{elapsed_time} seconds to merge")


# need form of [{word_key: word, title_vals: {title: n, ..., titleN: n}, {...}, {word_keyN: wordN, title_valsN: {title: n, ..., titleN: n}}}] for mongo
def word_json_to_json_arr(master_path, out_path):
    start_time = time.time()
    master_dict = load_dict_from_json(master_path)
    json_arr = [{"word_key": word_key, "title_vals": word_vals} for word_key, word_vals in master_dict.items()]
    write_dict_to_json(json_arr, out_path)
    elapsed_time = time.time() - start_time
    print(f"{elapsed_time} seconds to convert to json array")


# scrape writes json of {title: title_data} where title_data is an object
# need form of [{title_key: title, metadata: {...}, ..., {...}}] for mongo
def title_data_json_to_json_arr(title_data_path, out_path):
    start_time = time.time()
    title_data_dict = load_dict_from_json(title_data_path)
    json_arr = [{"title_key": title, "title_data": title_data} for title, title_data in title_data_dict.items()]
    write_dict_to_json(json_arr, out_path)
    elapsed_time = time.time() - start_time
    print(f"{elapsed_time} seconds to convert to json array")


# update word data json
def word_json_main():
    """
    previous_master = "data/master_dicts/02_13_2021_set0_0_100.json"
    new_master = "data/master_dicts/02_14_2021_set0_100_2100.json"
    merge_jsons(previous_master, new_master, out_master)
    """
    out_master = "data/master_dicts/02_14_2021_set0_0_2100.json"
    out_json_arr = "data/master_dict_json_arrays/02_14_2021_set0_0_2100.json"
    word_json_to_json_arr(out_master, out_json_arr)


# update title metadata json
def title_json_main():
    """
    previous_title = "data/title_data/02_13_2021_set0_0_100.json"
    new_title = "data/title_data/02_14_2021_set0_100_2100.json"
    merge_jsons(previous_title, new_title, out_title)
    """
    out_title = "data/title_data/02_14_2021_set0_0_2100.json"
    out_json_arr = "data/title_data_json_arrays/02_14_2021_set0_0_2100.json"
    title_data_json_to_json_arr(out_title, out_json_arr)


if __name__ == "__main__":
    word_json_main()
    title_json_main()


