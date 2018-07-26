from datetime import datetime
from dateutil import parser
import pytz
import sqlite3

temp = list()
conn = sqlite3.connect(r"c:\users\biswajit\coursera\twitter\database\tweets.db")
cursor = conn.cursor()


#get lowest id from database
sqlstmt = """SELECT id, create_date FROM tweets 
             ORDER BY id"""

rows = cursor.execute(sqlstmt)

for eachrow in rows:
    id = eachrow[0]
    tweet_date = parser.parse(eachrow[1],fuzzy=True)
#    eastern = pytz.timezone("US/Eastern")
#    print(tweet_date.astimezone(eastern))
    tweet_date = tweet_date.strftime("%Y-%m-%d %H:%M:%S%Z")
    temp.append((id,tweet_date))

for id,tweet_date in temp:
    try:
        cursor.execute("""UPDATE tweets
                      SET create_date = ?
                      WHERE id = ?""", (tweet_date, id))
    except Exception as e:
        print("Update failed", e)
        conn.rollback()
        break

conn.commit()