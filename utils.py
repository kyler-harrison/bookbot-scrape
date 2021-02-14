import re
import json
import nltk
from my_fake_useragent import UserAgent


# desc.: generate user agent headers for get requests
def gen_agent_headers():
    ua = UserAgent(family="chrome", os_family="windows")
    user_agent = ua.random()
    return {"User-Agent": user_agent}


# desc.: given string, remove any invalid characters 
# alpha_num: use alphanumeric only pattern (valid file name, use on the title)
def valid_chars(text, alpha_num=False):
    text = text.strip()
    if alpha_num:
        valid = re.compile("[a-zA-z0-9]")
    else:
        valid = re.compile("[ '.?!;a-zA-z0-9]")

    valid_text = ""
    for char in text:
        if re.match(valid, char):
            valid_text += char
        else:
            if not alpha_num:  # alpha num will include no spaces
                valid_text += " "  # weird whitespaces should be handled by preprocess model 
    return valid_text


# desc.: given word, return true if it contains valid characters, false if not
def valid_word(word, alpha=False):
    # should do some more checking on this regex
    if alpha:
        valid = re.compile("[a-zA-Z]")
    else:
        valid = re.compile("[-a-zA-z0-9'\.\- ]")
    for char in word:
        if not re.match(valid, char):
            return False
    return True


# desc.: given batch_size (number of processes to run at once) and a list of gr urls, return a list of lists like [[batch_size urls...], ..., [...]]
def batch_urls(batch_size, urls):
    batched_urls = []
    current_batch = []
    count = 0
    for url in urls:
        count += 1
        current_batch.append(url)
        if count % batch_size == 0:
            batched_urls.append(current_batch)
            current_batch = []

    # handle any left over (if the num urls provided isn't evenly divisible by the batch_size)
    if len(current_batch) > 0:
        batched_urls.append(current_batch)

    return batched_urls


# desc.: given string of text, return nltk tokenized list of sentence strings (is this even necessary?)
def breakdown_sentences(text):
    return nltk.sent_tokenize(text)


# desc.: given string of one review, returns list of sentences with valid_chars (is this even necessary?)
def review_breakdown(text):
    sentences_ls = breakdown_sentences(text)
    return [valid_chars(sentence).strip() for sentence in sentences_ls]


# desc.: given py dict and a json path, dump it
def write_dict_to_json(py_dict, dict_out_path):
    with open(dict_out_path, "w") as f:
        json.dump(py_dict, f)


# desc.: given json path, return py dictionary of the file
def load_dict_from_json(path):
    with open(path, "r") as f:
        return json.load(f)


# need form of [{word_key: word, title_vals: {title: n, ..., titleN: n}, {...}, {word_keyN: wordN, title_valsN: {title: n, ..., titleN: n}}}] for mongo
def word_json_to_json_arr(master_path, out_path):
    master_dict = load_dict_from_json(master_path)
    json_arr = [{"word_key": word_key, "title_vals": word_vals} for word_key, word_vals in master_dict.items()]
    write_dict_to_json(json_arr, out_path)


# scrape writes json of {title: title_data} where title_data is an object
# need form of [{title_key: title, metadata: {...}, ..., {...}}] for mongo
def title_data_json_to_json_arr(title_data_path, out_path):
    title_data_dict = load_dict_from_json(title_data_path)
    json_arr = [{"title_key": title, "title_data": title_data} for title_key, title_data in title_data_dict.items()]
    write_dict_to_json(json_arr, out_path)


# desc.: upload file to s3 bucket
# input: s3_resource, initialized boto3 thing
#        file_path, local path to the file to upload
#        extension, file extension used to define upload content (should be jpg or png)
#        file_key, name to save the file as in s3 bucket
#        bucket_name, name of the bucket in s3 to upload to
def upload_file_s3(s3_resource, file_path, extension, file_key, bucket_name):

    with open(file_path, "rb") as data_stream:
        try:
            file_key = "{}.{}".format(file_key, extension)
            cont_type = "image/{}".format(extension)
            # upload the data stream with public read permissions
            s3_resource.Bucket(bucket_name).put_object(Key=file_key, Body=data_stream, ContentType=cont_type, ACL="public-read")
        except:
            return False
    return True


# desc.: given list of csv files, concatenate all to one file, remove og files if you wish, enter "" for header if you don't want one
def combine_csv_files(files_ls, out_path, header, remove_files=False):
    out_file = open(out_path, "w")
    if header != "":
        out_file.write(header + "\n")
        for file_path in files_ls:
            in_file = open(file_path, "r")
            for line in in_file.readlines():
                out_file.write(line)
            in_file.close()

    if remove_files:
        for file_path in files_ls:
            os.remove(file_path)


