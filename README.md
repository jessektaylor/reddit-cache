# /cache_out.py

Collects the data processed by reddit-post-extraction and reddit-comment-extract and preprocess downsampling the data into hourly entries. This helps with loading pages with all the relevant data without having to query 10s thousands or  100s thousands rows and then process them on the fly. A month period is only about 720 rows or 720ish hours. Saved to a “redditpostcache”, and “redditcommentcache” tables. Combines all the tickers used dicts into one master dict for the hour.

# redditpostcache COLUMNS created with entries for every hours of cached data
  - title_tickers_used
  - text_tickers_used
  - post_title_polarity
  - post_title_subjectivity
  - post_text_polarity
  - post_text_subjectivity
  - datetime
  - subreddit


# REQUIRED ENVIRONMENT VARIABLES 
  - postgreshost=45.456.456.456
  - postgrespassword=Jfdgdfgdfgg

# Warning
- subjuctivity and polarity useless becuase it will just average out all the entries for the given hour and subreddit
- Must have extracted tables created and filled with data
