#! /usr/bin/env python
# coding:utf-8

from __future__ import division
import sys
import sqlite3
from fusha import Fusha

DBNAME = ":twitter:"

dbcon = sqlite3.connect(DBNAME)
# create conversation table
dbcur = dbcon.cursor()
dbcur.execute("create table twitter (id INTEGER PRIMARY KEY, \
            text TEXT, \
            created_at TEXT, \
            screen_name TEXT, \
            in_reply_to_status_id INTEGER, \
            in_reply_to_screen_name TEXT)")
dbcur.execute("create view reply as select * from twitter \
              where in_reply_to_status_id is not null")

with Fusha(title="creating twitter table"):
    for db in sys.argv[1:]:
        con = sqlite3.connect(db)
        # create conversation table
        cur = con.cursor()
        cur.execute("select * from twitter")
        for (tweet_id,
             text,
             created_at,
             screen_name,
             in_reply_to_status_id,
             in_reply_to_screen_name) in cur:
            dbcur.execute("insert into twitter values(? ,?, ?, ?, ?, ?)",
                          (tweet_id, text, created_at,
                           screen_name,
                           in_reply_to_status_id,
                           in_reply_to_screen_name))
    dbcon.commit()

with Fusha(title="creating reply table"):
    dbcur.execute("create table conversation \
                  (original TEXT, reply TEXT)")
    dbcur.execute("select * from reply")
    for (tweet_id,
         reply_text,
         created_at,
         screen_name,
         in_reply_to_status_id,
         in_reply_to_screen_name) in dbcur:

        # create conversation table
        lcur = dbcon.cursor()
        lcur.execute("select * from twitter")
        lcur.execute("select * from twitter where id=?",
                     (in_reply_to_status_id,))
        lcur = list(lcur)
        print len(lcur)
        for (tweet_id,
             orig_text,
             created_at,
             screen_name,
             in_reply_to_status_id,
             in_reply_to_screen_name) in lcur:
            wcur = dbcon.cursor()
            wcur.execute("insert into conversation values(?, ?)",
                         (orig_text, reply_text))
    dbcon.commit()
