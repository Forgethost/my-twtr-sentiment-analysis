import urllib.request, urllib.parse
from textblob import TextBlob
import hidden
import oauth2
import ssl
import json
import re
import sqlite3
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from datetime import datetime, timedelta
from dateutil import parser
import pytz

from sqlite3 import DatabaseError

sentiment = dict()
total_h1b = 0
total_count = 0
conn = sqlite3.connect(r"c:\users\biswajit\coursera\twitter\database\tweets.db")
cursor = conn.cursor()


#get lowest id from database
sqlstmt = """SELECT sentiment FROM tweets 
             ORDER BY tweet_id"""

rows = cursor.execute(sqlstmt)
#row = cursor.fetchone()

chart = open(r"c:\users\biswajit\coursera\twitter\piedata.js", "w")

chart.write("piedata=[ ['USCIS H1B Sentiment','count']")

for row in rows:
    total_count = total_count + 1
    if row[0] == None:
        continue
    try:
        sentiment[row[0]] = sentiment.get(row[0], 0) + 1
    except KeyError:
        pass
    except Exception as e:
        print(e)

labels = list()
sizes = list()

for key,values in sentiment.items():
    labels.append(key.capitalize())
    sizes.append(values)
    chart.write(",\n" + r"['" + key.capitalize() + r"'," + str(values) + r"]")
    total_h1b = total_h1b + values

chart.write("\n];\n")
chart.write("postdata=[ ['Type of posts','count']")
chart.write(",\n" + r"['" + "Other posts" + r"'," + str(total_count) + r"]")
chart.write(",\n" + r"['" + "H1B posts" + r"'," + str(total_h1b) + r"]")
chart.write("\n];\n")
chart.close()

def general_chart(labels, label1, sizes, sizes1):
    fig1, (ax1,ax2) = plt.subplots(1,2)
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=90)
#plt.pie(sizes, labels=labels, autopct='%1.1f%%',
#        shadow=True, startangle=90)
    ax1.axis('equal')
    ax1.set_title("H1B Sentiment analysis")
    ax2.pie(sizes1, labels=label1, autopct='%1.1f%%',
        shadow=True, startangle=90)
#plt.pie(sizes, labels=labels, autopct='%1.1f%%',
#        shadow=True, startangle=90)
    ax2.axis('equal')
    ax2.set_title("USCIS H1B posts")
    plt.show()


def chart(cursor, days):
    total_h1b = 0
    total_count = 0
    sentiment = dict()
    utc = pytz.timezone("UTC")
    curr_date = utc.localize(datetime.today())
    sqlstmt = """SELECT tweet_id, create_date FROM tweets
                ORDER BY create_date DESC LIMIT 1"""
    cursor.execute(sqlstmt)
    row = cursor.fetchone()
    if row == None:
        pass
    else:
        last_entry_date = parser.parse(row[1])
        last_tweet_id = row[0]
        diff = curr_date - last_entry_date
        try:
            start_date = curr_date - timedelta(days=days)
            start_date_str = start_date.strftime("%Y-%m-%d")
#            print(start_date_str)
            sqlstmt = """SELECT create_date, sentiment FROM tweets 
                            WHERE create_date >= ? 
                            ORDER BY create_date"""
            rows = cursor.execute(sqlstmt, (start_date_str,))
            for eachrow in rows:
                total_count = total_count + 1
                if eachrow[1] == None:
                    continue
                try:
                    sentiment[eachrow[1]] = sentiment.get(eachrow[1], 0) + 1
                except KeyError:
                    pass
                except Exception as e:
                    print(e)

            labels = list()
            sizes = list()

#            print(sentiment)
            for key, values in sentiment.items():
                labels.append(key.capitalize())
                sizes.append(values)
                total_h1b = total_h1b + values

            labels1 = ["H-1B", "Total USCIS Posts"]
            sizes1 = [total_h1b, total_count]

            plt.subplot(1,2,1)
            plt.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
            plt.title("H-1B Sentiment in last {} days - USCIS Twitter posts".format(days))
            plt.axis("equal")

            plt.subplot(1,2,2)
            plt.pie(sizes1, labels=labels1, autopct='%1.1f%%', shadow=True, startangle=90)
            plt.title("H-1B posts @USCIS in Twitter - last {} days".format(days))
            plt.axis("equal")
            plt.show()

        except Exception as e:
            print("Unexpected error", e)

        return

#        print(start_date_str)


def main():
    label1 = "Other posts", "H1B posts"
    sizes1 = [total_count, total_h1b]
    while True:
        days = input("Enter Number of past days to view analytics: ")
        try:
            days = int(days)
            if days > 30:
                print("Only 30 days analytics look back is allowed..please enter within 30 days")
                continue
            else:
                break
        except:
            print("Please enter days as number only")
            continue
#    general_chart(labels, label1, sizes, sizes1)
    chart(cursor, days)
    cursor.close()


#fig1, ax1 = plt.subplots()
#ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
#        shadow=True, startangle=90)
#plt.pie(sizes, labels=labels, autopct='%1.1f%%',
#        shadow=True, startangle=90)
#ax1.axis('equal')
#plt.subplot(the_grid[0, 1], aspect=1)
#ax2.pie(sizes1, labels=label1, autopct='%1.1f%%',
#        shadow=True, startangle=90)
#ax2.axis('equal')
#plt.pie(sizes1, labels=label1, autopct='%1.1f%%',
#        shadow=True, startangle=90)
#plt.title('H1B Sentiment analysis - USCIS Twitter post')


if __name__ == "__main__":
    main()

