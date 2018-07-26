import urllib.request, urllib.parse
from textblob import TextBlob
import hidden
import oauth2
import ssl
import json
import re
from dateutil import parser
import sqlite3
from sqlite3 import DatabaseError

conn = sqlite3.connect(r"c:\users\biswajit\coursera\twitter\database\tweets.db")
cursor = conn.cursor()

secrets = hidden.auth_secret()
def oauth_req(url, key, secret, http_method="GET", post_body="".encode(), http_headers=None):
    consumer = oauth2.Consumer(key=secrets["consumer_key"], secret=secrets["consumer_secret"])
    token = oauth2.Token(key=key, secret=secret)
    client = oauth2.Client(consumer, token)
    resp, content = client.request( url, method=http_method, body=post_body, headers=http_headers )
#    print(resp)
    return resp, content

def load_table(cursor, data_list):
    """This function loads tweets table with the data
       passed in the list of tuples """

    sqlstmt = """INSERT OR REPLACE INTO tweets(tweet_id, create_date, user_id, user_name,
                                    location, url, tweet_text, sentiment)
                VALUES(?,?,?,?,?,?,?,?)"""
    for datarow in data_list:
        try:
            cursor.execute(sqlstmt,(datarow))
            print("Record inserted")
        except Exception as e:
            print(e)
            raise DatabaseError
            break


#get lowest id from database
sqlstmt = """SELECT tweet_id FROM tweets 
             ORDER BY tweet_id DESC LIMIT 1"""

cursor.execute(sqlstmt)
row = cursor.fetchone()


if row != None:
    max_id = row[0]



# https://apps.twitter.com/
# Create App and get the four strings, put them in hidden.py

#TWITTER_URL = 'https://api.twitter.com/1.1/friends/list.json'
base_url = "https://api.twitter.com/1.1/search/tweets.json?"
query_str = "q=" + urllib.parse.quote("from:USCIS") + "&"
twitter_url = base_url + query_str + urllib.parse.urlencode({"result_type":"mixed","count":50})


if max_id is None:
    pass
else:
#    twitter_url = twitter_url + "&" + urllib.parse.urlencode({"max_id": max_id})
    twitter_url = twitter_url + "&" + urllib.parse.urlencode({"since_id": max_id})

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


try_count = input("Enter number of iterations: ")
try:
    try_count = int(try_count)
except:
    print("Enter numeric value")
    quit()
if try_count > 150:
    print("Max 150 iteration possible in 15 minute window,,setting iteration count to 150")
    try_count = 150
u_str = r".*\u"
iter_count = 0
fail_count = 0
response = list()


test = r"""RT @ICEgov: Joint Operation nets #ICE 24 transnational gang members, 475 total arrests under Operation Matador https://t.co/4TEV4qmFO"""
#print(re.findall("@+[A-Za-z0-9]+:(.+)", test))
#print(re.sub("#\S+", "", test))



while True:
    if iter_count >= try_count:
        print("End of iteration")
        break

    print("Retrieving", twitter_url)
    resp, content = oauth_req(twitter_url, secrets["token_key"], secrets["token_secret"])
    iter_count = iter_count + 1
    result_header = resp
#    print(result_header)
    if result_header == None or int(result_header["content-length"]) == 0 or int(result_header["status"]) != 200:
        print("No more data retrieved url:", url, "iteration:", iter_count)
        break
    try:
        result_content = json.loads(content)
    except:
        print("Json Load failed, iteration count", iter_count)
        fail_count = fail_count + 1
        continue

    if fail_count >= 2:
        print("Fail tolerance exceeded, fail count", fail_count, "iteration:", iter_count)
        break
    print("Limit remaining", result_header['x-rate-limit-remaining'])
    print("Limit Rate", result_header['x-rate-limit-limit'])

    if iter_count == (int(result_header['x-rate-limit-remaining'])-50):
        print("Maximum limit reached..")
        break

    for eachresp in result_content["statuses"]:
        print(eachresp)
        create_date = eachresp["created_at"]
        id = eachresp["id"]
        user_id = eachresp["user"]["id"]
        user_name = eachresp["user"]["name"]
        location = eachresp["user"]["location"]
        try:
            url = eachresp["entities"]["urls"][0]["url"]
        except:
            url=None

        resp_text = "%r" % eachresp["text"]

        if re.search("@+[A-Za-z0-9]+:.+", resp_text):
            tweet_text = re.findall("@+[A-Za-z0-9]+:(.+)", resp_text)[0][:-1]
        else:
            tweet_text = resp_text[:-1]
        if re.search("(.*\S+?).+http", resp_text):
            tweet_text = re.findall("(.*\S+?).+http", resp_text)[0][:-1]

        tweet_text = re.sub("http\S+", "", tweet_text)
        tweet_text = re.sub("#\S+", "", tweet_text)

        print(tweet_text)
        if re.search("H-1B",tweet_text):
            analysis = TextBlob(tweet_text)
            if analysis.sentiment.polarity > 0:
                opinion = "positive"
            elif analysis.sentiment.polarity == 0:
                opinion = "neutral"
            else:
                opinion = "negetive"
        else:
            opinion = None
        create_date = parser.parse(create_date, fuzzy=True)
        create_date = create_date.strftime("%Y-%m-%d %H:%M:%S%Z")
        response.append((id, create_date, user_id, user_name, location, url, tweet_text, opinion))

    try:
        load_table(cursor, response)
        conn.commit()
    except DatabaseError:
        print("Error loading tweets table")
        conn.rollback()
        break
    except:
        print("Unknown error")
        conn.rollback()
        break
#    print(response)

#print("End of iteration")


#print(json.dumps(result_content,indent=2))
cursor.close()