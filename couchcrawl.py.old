#! /usr/bin/env python
# coding:utf-8

import couchdb
import datetime
import twitter as tw
import traceback as tb
import chartype


def is_valid_char(st):
    ch = chartype.Chartype()
    try:
        return ch.is_hiragana(st) or \
            ch.is_katakana(st)
    except ValueError:
        return False


def is_valid_text(text):
    for st in text:
        if is_valid_char(st):
            return True
    else:
        return False


class Crawl(object):
    def __init__(self,
                 dbname,
                 consumer_key,
                 consumer_secret,
                 oauth_token,
                 oauth_secret):
        self.dbname = dbname
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.oauth_token = oauth_token
        self.oauth_secret = oauth_secret
        self.auth = tw.OAuth(self.oauth_token,
                             self.oauth_secret,
                             self.consumer_key,
                             self.consumer_secret)

    def usercrawl(self):
           # see "Authentication" section below for tokens and keys

        twitter_stream = tw.TwitterStream(
            auth=self.auth,
            #api_version="1.1",
            domain="userstream.twitter.com")

        db = couchdb.Server('http://localhost:5984')
        twitterdb = db[self.dbname]

        print "Connection Twitter started ..."
        for status in twitter_stream.user():
            try:
                twitterdb.save(dict(status))
                print "update CouchDB at {time}".format(
                    time=datetime.datetime.now())
            except tw.TwitterHTTPError:
                tb.print_exc()

    def samplecrawl(self):

        # see "Authentication" section below for tokens and keys
        twitter_stream = tw.TwitterStream(auth=self.auth)

        db = couchdb.Server('http://localhost:5984')
        twitterdb = db[self.dbname]

        print "Connection Twitter started ..."
        for status in twitter_stream.statuses.sample():
            try:
                st = status[u'text']
                if is_valid_text(st):
                    try:
                        twitterdb.save(dict(status))
                        print "update CouchDB at {time}".format(
                            time=datetime.datetime.now())
                    except tw.TwitterHTTPError:
                        tb.print_exc()
            except:
                print "no text field"
