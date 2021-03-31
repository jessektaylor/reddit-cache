

import psycopg2
import os
import pandas as pd
import datetime 
import ast
# 
class CacheOutPosts(object):

    def __init__(self):
        self.total_title_tickers = dict()
        self.chunk_title_tickers_used = dict()
        self.chunk_text_tickers_used = dict()
        self.chunk_post_title_subjectivity = 0
        self.chunk_post_title_polarity= 0
        self.chunk_post_text_polarity = 0
        self.chunk_post_text_subjectivity = 0


    def __enter__(self):
        self.conn = psycopg2.connect(
                host= os.getenv('postgreshost'),
                database="postgres",
                user="postgres",
                password=os.getenv('postgrespassword'))
        self.conn.autocommit=True
        self.curr = self.conn.cursor()
        # Create database tables
        self.create_reddit_post_cache_table()
       
        for i, chunk in enumerate(self.post_generator()):
            self.post_chunk_process(chunk)
    

    def post_chunk_process(self, chunk):
        # iterate through subreddits present in the chunk
        subreddits = set(chunk['subreddit'])
        for subreddit in subreddits:
            self.subreddit = subreddit
            #iterate through rows of chunks with unqiue subreddit dataframes
            for index, row in chunk[chunk.subreddit.isin([subreddit])].iterrows():
                # process columns in the rows
                self.update_title_tickers_used(row)
                self.update_text_tickers_used(row)
                self.update_post_polarilty_subectivity(row)
            print(self.chunk_text_tickers_used)
            # save the cache items
            self.save_post_cache_db()
            # reset the varibles
            self.chunk_title_tickers_used = dict()
            self.chunk_text_tickers_used = dict()


    def update_post_polarilty_subectivity(self, row):
        self.chunk_post_title_subjectivity += row['post_title_subjectivity']
        self.chunk_post_title_polarity += row['post_title_polarity']
        self.chunk_post_text_polarity += row['post_text_polarity']
        self.chunk_post_text_subjectivity += row['post_text_subjectivity']


    def update_text_tickers_used(self, row):
        text_tickers_used = ast.literal_eval(row['text_tickers_used'])
        for ticker in text_tickers_used:
            text_tickers_used[ticker]
            try:
                sofar = self.chunk_text_tickers_used[ticker]
                self.chunk_text_tickers_used[ticker] = sofar + text_tickers_used[ticker]
            except KeyError:
                self.chunk_text_tickers_used[ticker] = text_tickers_used[ticker]


    def update_title_tickers_used(self, row):
        title_tickers_used = ast.literal_eval(row['title_tickers_used'])
        for ticker in title_tickers_used:
            title_tickers_used[ticker]
            try:
                sofar = self.chunk_title_tickers_used[ticker]
                self.chunk_title_tickers_used[ticker] = sofar + title_tickers_used[ticker]
            except KeyError:
                self.chunk_title_tickers_used[ticker] = title_tickers_used[ticker]


    def post_generator(self):
        self.curr.execute("""
            SELECT min(datetime) 
            FROM redditpost
            ;""")
        first_date = self.curr.fetchone()[0]
        d = datetime.datetime.now().replace(minute=0,second=0, microsecond=0)
        start_date =  d + datetime.timedelta(minutes=60)
        # loop subtracking one hour until first_date where item was saved is reached
        while start_date > first_date:
            self.chunk_time_start = start_date - datetime.timedelta(hours=1)
            self.curr.execute("""
                SELECT
                redditpost.datetime,
                redditpost.subreddit,
                redditpost.id,
                redditpostliteralextraction.title_tickers_used,
                redditpostliteralextraction.text_tickers_used,
                redditpostliteralextraction.post_title_polarity,
                redditpostliteralextraction.post_title_subjectivity,
                redditpostliteralextraction.post_text_polarity,
                redditpostliteralextraction.post_text_subjectivity
                FROM redditpost
                INNER JOIN redditpostliteralextraction ON
                redditpost.id=redditpostliteralextraction.postid
                WHERE datetime BETWEEN
                %s and %s 
            ;""",[self.chunk_time_start, start_date])
            self.chunk_time_end = start_date
            chunk = self.curr.fetchmany(100000)
            start_date = start_date - datetime.timedelta(hours=1)
            if chunk:
                columns = ['datetime',
               'subreddit',
                'id',
                'title_tickers_used',
                'text_tickers_used',
                'post_title_polarity',
                'post_title_subjectivity',
                'post_text_polarity',
                'post_text_subjectivity']
                df = pd.DataFrame(chunk, columns=columns).fillna(value=0)
                yield df


    def create_reddit_post_cache_table(self):
        self.curr.execute("""CREATE TABLE IF NOT EXISTS redditpostcache (
                id serial,
                title_tickers_used varchar(3000) NOT NULL,
                text_tickers_used varchar(3000) NOT NULL,
                post_title_polarity float NOT NULL,
                post_title_subjectivity float NOT NULL,
                post_text_polarity float NOT NULL,
                post_text_subjectivity float NOT NULL,
                datetime timestamp NOT NULL,
                subreddit varchar(255) 
            );""")


    def save_post_cache_db(self):
        self.curr.execute("""SELECT id FROM redditpostcache
                WHERE datetime=(%s) AND subreddit=(%s)""", (self.chunk_time_end,self.subreddit,))
        postcache_qury = self.curr.fetchone()
        if postcache_qury == None: # create one if one is not found
            self.curr.execute("""INSERT INTO redditpostcache
                        (title_tickers_used,
                        text_tickers_used,
                        post_title_polarity,
                        post_title_subjectivity,
                        post_text_polarity,
                        post_text_subjectivity,
                        datetime,
                        subreddit)
                        VALUES (%s, %s, %s, %s, %s, %s,%s, %s)
                        """,
                        (str(self.chunk_title_tickers_used),
                        str(self.chunk_text_tickers_used),
                        str(self.chunk_post_title_polarity),
                        self.chunk_post_title_subjectivity,
                        self.chunk_post_text_polarity,
                        self.chunk_post_text_subjectivity,
                        self.chunk_time_end,
                        self.subreddit))
        else: # post is not None update information
            self.curr.execute("""UPDATE redditpostcache SET
                        title_tickers_used =%s,
                        text_tickers_used=%s,
                        post_title_polarity=%s,
                        post_title_subjectivity=%s,
                        post_text_polarity=%s,
                        post_text_subjectivity=%s,
                        datetime=%s,
                        subreddit=%s
                        WHERE id=%s
                        """,
                        (str(self.chunk_title_tickers_used),
                        str(self.chunk_text_tickers_used),
                        str(self.chunk_post_title_polarity),
                        str(self.chunk_post_title_subjectivity),
                        self.chunk_post_text_polarity,
                        self.chunk_post_text_subjectivity,
                        self.chunk_time_end,
                        self.subreddit,
                        postcache_qury))


    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
        self.curr.close()
        print('finish Posts cache updates')
    

class CacheOutComments(object):

    def __init__(self):
        self.total_tickers, self.total_companies = dict(), dict()
        self.chunk_comment_tickers_used = dict()
        self.chunk_comment_polarity = 0
        self.chunk_comment_subjectivity = 0


    def __enter__(self):
        self.conn = psycopg2.connect(
                host= os.getenv('postgreshost'),
                database="postgres",
                user="postgres",
                password=os.getenv('postgrespassword'))
        self.conn.autocommit=True
        self.curr = self.conn.cursor()
        # Create database tables

        self.create_reddit_comment_cache_table()
        
        for i, chunk in enumerate(self.comment_generator()):
            self.post_chunk_process(chunk)


    def post_chunk_process(self, chunk):
        # iterate through subreddits present in the chunk
        subreddits = set(chunk['subreddit'])
        for subreddit in subreddits:
            self.subreddit = subreddit
            #iterate through rows of chunks with unqiue subreddit dataframes
            for index, row in chunk[chunk.subreddit.isin([subreddit])].iterrows():
             
                # process columns in the rows
                self.update_comment_tickers_used(row)
                self.update_comment_polarilty_subectivity(row)
                
            # save the cache items
            print(self.chunk_comment_tickers_used)
            self.save_comment_cache_db()

            # reset the varibles
            self.chunk_comment_tickers_used = dict()


    def update_comment_polarilty_subectivity(self, row):
        self.chunk_comment_subjectivity += row['comment_subjectivity']
        self.chunk_comment_polarity += row['comment_polarity']


    def update_comment_tickers_used(self, row):
        comment_tickers_used = ast.literal_eval(row['comment_tickers_used'])
        for ticker in comment_tickers_used:
          
            try:
                sofar = self.chunk_comment_tickers_used[ticker]
                self.chunk_comment_tickers_used[ticker] = sofar + comment_tickers_used[ticker]
            except KeyError:
                self.chunk_comment_tickers_used[ticker] = comment_tickers_used[ticker]


    def comment_generator(self):
        self.curr.execute("""
            SELECT min(datetime) 
            FROM redditcomment
            ;""")
        first_date = self.curr.fetchone()[0]
        d = datetime.datetime.now().replace(minute=0,second=0, microsecond=0)
        start_date =  d + datetime.timedelta(minutes=60)
        # loop subtracking one hour until first_date where item was saved is reached
        while start_date > first_date:
            self.chunk_time_start = start_date - datetime.timedelta(hours=1)
            self.curr.execute("""
                SELECT
                redditcomment.datetime,
                redditcomment.subreddit,
                redditcomment.id,
                redditcommentliteralextraction.comment_tickers_used,
                redditcommentliteralextraction.comment_polarity,
                redditcommentliteralextraction.comment_subjectivity
                FROM redditcomment
                INNER JOIN redditcommentliteralextraction ON
                redditcomment.id=redditcommentliteralextraction.commentid
                WHERE datetime BETWEEN
                %s and %s 
            ;""",[self.chunk_time_start, start_date])
            self.chunk_time_end = start_date
            chunk = self.curr.fetchmany(100000)
            start_date = start_date - datetime.timedelta(hours=1)
            if chunk:
                columns = ['datetime',
                'subreddit',
                'id',
                'comment_tickers_used',
                'comment_polarity',
                'comment_subjectivity']
                df = pd.DataFrame(chunk, columns=columns).fillna(value=0)
                yield df

 
    def create_reddit_comment_cache_table(self):
        self.curr.execute("""CREATE TABLE IF NOT EXISTS redditcommentcache (
                id serial,
                comment_tickers_used varchar(300000) NOT NULL,
                comment_polarity float NOT NULL,
                comment_subjectivity float NOT NULL,
                datetime timestamp NOT NULL,
                subreddit varchar(255)
            );""")


    def save_comment_cache_db(self):
        # check if comment exists
        self.curr.execute("""SELECT id FROM redditcommentcache
                        WHERE datetime=(%s) AND subreddit=(%s)  """, (self.chunk_time_end,self.subreddit))
        comment_id = self.curr.fetchone()
        if comment_id == None: # create one if one is not found
            self.curr.execute("""INSERT INTO redditcommentcache
                        (comment_tickers_used, 
                        comment_polarity, 
                        comment_subjectivity,
                        datetime,
                        subreddit)
                        VALUES (%s , %s, %s, %s, %s)
                        """,
                        (str(self.chunk_comment_tickers_used), 
                        self.chunk_comment_polarity, 
                        self.chunk_comment_subjectivity,
                        self.chunk_time_end,
                        self.subreddit,))
        else: # comment is not None update information
            self.curr.execute("""UPDATE redditcommentcache SET
                        comment_tickers_used= %s,
                        comment_polarity = %s,
                        comment_subjectivity = %s,
                        datetime= %s,
                        subreddit= %s
                        WHERE id=%s
                        """,
                        (str(self.chunk_comment_tickers_used),
                        self.chunk_comment_polarity,
                        self.chunk_comment_subjectivity,
                        self.chunk_time_end,
                        self.subreddit,
                        comment_id,))
     

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
        self.curr.close()
        print('finish Comments cache updates')


with CacheOutPosts() as create_cache:
    create_cache
    
with CacheOutComments() as create_cache:
    create_cache