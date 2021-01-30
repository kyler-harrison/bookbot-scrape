import re
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
