import boto3
import botocore.exceptions
import json
import time
import datetime
from utils import *


# NOTE it is assumed that the dictionaries will not have overlapping titles, which they shouldn't so long as you check for this each time 
# you add new urls in link_scrape.py (you have for the ~9200 collected on 1/29)
# desc.: if you have a previous master dict json, and you scrape some new data and save that master dict somewhere else, this
#        will update the previous master dict with the new master dict data and then write to out_master_json_path
def update_local_master_json(previous_master_json_path, new_master_json_path, out_master_json_path):
    start_time = time.time()

    prev_dict = load_dict_from_json(previous_master_json_path)
    new_dict = load_dict_from_json(new_master_json_path)

    for word_key, title_freq_dict in new_dict.items():
        try:
            # existing words dict updates with the new {title: 0.1} 
            prev_dict[word_key].update(title_freq_dict)
        except KeyError:
            # if the word in the new dict doesn't exist, add it to the previous and put its dict in 
            prev_dict[word_key] = title_freq_dict

    write_dict_to_json(prev_dict, out_master_json_path)

    elapsed_time = time.time() - start_time
    print(f"{elapsed_time} seconds to update local master")


# desc.: add field (probably amz_link) to aws dynamo db that contains metadata on the books 
# inputs:
#        title_key: the title of the book to update, ex.: "TheKiteRunner"
#        value_key: the column name/field to add, ex.: "amz_link"
#        value: the value to add to the value_key in the aws dict format, ex.: {"S": "urmom.com"}
#        db_client: boto3 dynamo db client
#        db_table_name: table name to update info in
# outputs:
#        returns True if successful update, False otherwise
def update_metadata_db(title_key, value_key, value, db_client, db_table_name):
    try:
        response = db_client.update_item(TableName=db_table_name, 
                                         Key={"title": {"S": f"{title_key}"}},
                                         UpdateExpression=f"set {value_key} = :v",
                                         ExpressionAttributeValues={":v": value})
        return True
    except:
        return False


# desc.: update an entry in the aws dynamo master dict db, returns True if the key exists, False else
# input:
#        word_key, primary word key to add data to 
#        word_freq, deceptive name, needs to {"N": "0.005"} (handled in the upload function)
#        word_title, string of the title that corresponds to the word_freq above (just a string)
#        db_client, aws dynamo db client
#        db_table_name, aws dynamo db table name of the running master dict
def update_word_db(word_key, word_freq, word_title, db_client, db_table_name):
    try:
        response = db_client.update_item(TableName=db_table_name,
                                         Key={"word_key": {"S": f"{word_key}"}},
                                         UpdateExpression="set title_freqs.#title = :newdict",
                                         ExpressionAttributeNames={"#title": f"{word_title}"},
                                         ExpressionAttributeValues={":newdict": word_freq})
        return True

    # this should only happen when the word doesn't exist, need import at top to catch this
    except botocore.exceptions.ClientError:
        return False


# desc.: given a master dict json, update or upload the values in each word key. updates one by one if the word exists, uploads in a batch if doesn't.
# input: 
#        dict_path, path to the json of the master dict in the form {"word_key": {"title": 0.07}}
#        db_client, aws dynamo client
#        db_table_name, name of the aws dynamo table that contains the master dict data
def upload_dict(dict_path, db_client, db_table_name):
    start_time = time.time()

    # load master dict
    with open(dict_path, "r") as f:
        the_dict = json.load(f)

    # go through each word key in the given master dict and update/upload
    for word_key, word_vals in the_dict.items():

        word_exists = True  # initially assume word exists (flips if the first update fails)
        sub_entry = {}  # if updateable, this shouldn't get defined
        for title_key, title_freq in word_vals.items():

            # if the word exists in the db, only this should run
            if word_exists:
                title_freq_aws_dict = {"N": str(title_freq)}
                word_exists = update_word_db(word_key, title_freq_aws_dict, title_key, db_client, db_table_name)

            # not using an else because of the first case
            if not word_exists:
                sub_entry.update({title_key: {"N": str(title_freq)}})

        # push map up if the word didn't exist
        if not word_exists:
            entry = {"word_key": {"S": word_key}}
            entry.update({"title_freqs": {"M": sub_entry}})
            db_client.put_item(TableName=db_table_name, Item=entry)
    
    elapsed_time = time.time() - start_time
    print(f"time taken to upload: {elapsed_time} seconds")


def upload_dict_tester(dict_path, db_client, db_table_name):
    start_time = time.time()

    # load master dict
    with open(dict_path, "r") as f:
        the_dict = json.load(f)

    print(f"master dict len = {len(the_dict)}")

    # go through each word key in the given master dict and update/upload
    count = 0
    for word_key, word_vals in the_dict.items():
        count += 1

        sub_entry = {}  # if updateable, this shouldn't get defined
        for title_key, title_freq in word_vals.items():
            sub_entry.update({title_key: {"N": str(title_freq)}})

        entry = {"word_key": {"S": word_key}}
        entry.update({"title_freqs": {"M": sub_entry}})
        db_client.put_item(TableName=db_table_name, Item=entry)

        if count == 100:
            break
    
    elapsed_time = time.time() - start_time
    print(f"time taken to upload: {elapsed_time} seconds")


def upload_dict_err(dict_path, db_client, db_table_name):
    start_time = time.time()

    # load master dict
    with open(dict_path, "r") as f:
        the_dict = json.load(f)

    # go through each word key in the given master dict and update/upload
    count = 0
    for word_key, word_vals in the_dict.items():
        count += 1

        word_exists = True  # initially assume word exists (flips if the first update fails)
        sub_entry = {}  # if updateable, this shouldn't get defined
        for title_key, title_freq in word_vals.items():

            # if the word exists in the db, only this should run
            if word_exists:
                title_freq_aws_dict = {"N": str(title_freq)}
                word_exists = update_word_db(word_key, title_freq_aws_dict, title_key, db_client, db_table_name)

            # not using an else because of the first case
            if not word_exists:
                sub_entry.update({title_key: {"N": str(title_freq)}})

        # push map up if the word didn't exist
        if not word_exists:
            entry = {"word_key": {"S": word_key}}
            entry.update({"title_freqs": {"M": sub_entry}})
            db_client.put_item(TableName=db_table_name, Item=entry)
        
        if count == 100:
            break
    
    elapsed_time = time.time() - start_time
    print(f"time taken to upload: {elapsed_time} seconds")


# NOTE my findings: straight upload (no update check) for 100 word keys takes ~4.5 seconds
# with update takes ~9 seconds
# based on the ~4.5 seconds for 100, a worst case of 1e6 entries would take about 750 hours, if you could multiprocess maybe 75 hours
# really need to see if I can just upload a json in a few minutes - which makes sense, right?
def dontrun():
    date_now = datetime.datetime.now().strftime("%m_%d_%Y")  # current date for file save

    # aws vars
    db_client = boto3.client("dynamodb", region_name="us-east-1")
    db_table_name = "bookOracleMasterDict"

    # TODO vars to change each time
    master_base = "data/master_dict/"
    #previous_master_json_path = f"{master_base}"
    new_master_name = "02_03_2021_initial_100_test_set0"
    new_master_json_path = f"{master_base}{new_master_name}.json"
    #out_master_json_path = f"{date_now}_current_db.json"

    # upload/update data in db
    upload_dict_err(new_master_json_path, db_client, db_table_name)

    # update the local master json to match data in db
    #update_local_master_json(previous_master_json_path, new_master_json_path, out_master_json_path)


    # sample metadata add
    #title_key = "TheMidnightLibrary"
    #value_key = "amz_link" 
    #value = {"S": "urdad.com"}
    #update_metadata_db(title_key, value_key, value, db_client, db_table_name)

    # sample update
    # db_table_name = "bookOracleMasterDict"
    # word_key = "urmom"
    # word_freq = {"N": str(0.3)}
    # word_title = "cmonmate"

    # upload the local master dict json

def merger_main():
    # TODO change these three vars to merge old/new to current master json
    previous_master_json_path = ""
    new_master_json_path = ""
    out_master_json_path = ""

    update_local_master_json(previous_master_json_path, new_master_json_path, out_master_json_path)


if __name__ == "__main__":
    main()


