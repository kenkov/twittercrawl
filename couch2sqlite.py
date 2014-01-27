#! /usr/bin/env python
# coding:utf-8

#
# Last Updated on 2012/04/20 03:38:36 .
#
import couchdb
import sqlite3
import traceback
import sys


name = sys.argv[1]
TWITTERDB = "".join([":", name, ":"])


# sqlite3 のテーブルの作成
def create_twitter_table():
    con = sqlite3.connect(TWITTERDB)
    cur = con.cursor()
    try:
        cur.execute("""create table twitter
                    (id INGETER PRIMARY KEY,
                    text TEXT,
                    created_at TEXT,
                    screen_name TEXT,
                    in_reply_to_status_id INTEGER,
                    in_reply_to_screen_name TEXT)""")

    except sqlite3.Error:
        pass

    db = couchdb.Server('http://localhost:5984')
    twitterdb = db[name]

    for id in twitterdb:
        try:
            twi = twitterdb[id]
            tweet_id = int(twi['id_str'])
            text = twi['text']
            created_at = twi['created_at']
            screen_name = twi['user']['screen_name']
            in_reply_to_status_id = twi['in_reply_to_status_id']
            in_reply_to_screen_name = twi['in_reply_to_screen_name']
            cur.execute("insert into twitter values(? ,?, ?, ?, ?, ?)",
                        (tweet_id,
                         text,
                         created_at,
                         screen_name,
                         in_reply_to_status_id,
                         in_reply_to_screen_name))
            con.commit()
            print u'{id} INSERTED\n \
                      {text}\n \
                      created_by: {created_by}'.format(id=tweet_id,
                                                       text=text,
                                                       created_by=screen_name)
        except KeyboardInterrupt:
            traceback.print_exc()
            break
        except:
            traceback.print_exc()
    cur.close()


def create_reply_view():
    # reply ありのツイートのテーブルを作成
    con = sqlite3.connect(TWITTERDB)
    cur = con.cursor()
    try:
        cur.execute("drop view reply")
    except sqlite3.Error:
        print "reply view does not exists. creating a new view"

    cur.execute("""create view reply as select * from twitter
                where in_reply_to_status_id is not null""")
    con.commit()
    cur.close()

if __name__ == "__main__":
    create_twitter_table()
    create_reply_view()
