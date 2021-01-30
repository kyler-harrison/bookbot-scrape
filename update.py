import boto3
import json
from utils import *


# TODO function to update existing words in db
# pseudo:
# given new master dict (whichever you're currently on with scrape), loop through word_key, title_props {title: 0.1, ..., titleN: 0.2}
# response = aws_update(word_key, title_props)
# if response successfully updated -> cool
# else just do what you'd do if uploading -> aws_put(word_key, title_props) 
def update_word_db():
    pass


# desc.: add field to (probably amz_link) to aws dynamo db that contains metadata on the books 
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



# INITIAL UPLOAD ONLY
# desc.: given path to master dict, dynamo client and db table name, update the aws word db
def upload_dict(dict_path, db_client, db_table_name):
    with open(dict_path, "r") as f:
        the_dict = json.load(f)

    # TODO once good, upload everybody and run some more tests
    # don't want these guys in the db
    try:
        # TODO these shouldn't be in there anymore so shouldn't need to do this
        the_dict.pop("TOTAL_UNIQUE_COUNT")
        the_dict.pop("TOTAL_WORD_COUNT")
    except KeyError:
        pass

    count = 0
    for word_key, word_val_dict in the_dict.items():
        count += 1
        sub_entry = {}
        for title_key, title_freq in word_val_dict.items():
            sub_entry.update({title_key: {"N": str(title_freq)}})
            print("sub: {}\n".format(sub_entry))
        entry = {"word_key": {"S": word_key}}
        entry.update({"title_freqs": {"M": sub_entry}})
        print("full: {}".format(entry))
        db_client.put_item(TableName=db_table_name, Item=entry)
        if count == 20:
            break


# TODO make sure you have removed TOTAL_UNIQUE_COUNT and TOTAL_WORD_COUNT before you write these in the first place (why do you even need?)
# NOTE it is assumed that the dictionaries will not have overlapping titles, which they shouldn't so long as you check for this each time 
# you add new urls in link_scrape.py (you have for the ~9200 collected on 1/29)
# desc.: if you have a previous master dict json, and you scrape some new data and save that master dict somewhere else, this
#        will update the previous master dict with the new master dict data and then write to out_master_json_path
#        TODO now take the new master dict and update the db (need a function to update if exists and add if doesn't)
def update_local_master_json(previous_master_json_path, new_master_json_path, out_master_json_path):
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


def main():
    db_client = boto3.client("dynamodb", region_name="us-east-1")
    db_table_name = "bookOracleData"

    # sample metadata add
    title_key = "TheMidnightLibrary"
    value_key = "amz_link" 
    value = {"S": "urdad.com"}
    update_metadata_db(title_key, value_key, value, db_client, db_table_name)


if __name__ == "__main__":
    main()


