# https://github.com/JoMingyu/google-play-scraper/blob/master/README.md#google-play-scraper

from google_play_scraper import reviews_all, Sort
import pandas as pd
import random
import time


def fetch_reviews_with_retry(app_id, max_retries=5, initial_delay=500):
    retries = 0
    delay = initial_delay
    while retries < max_retries:
        try:
            reviews = reviews_all(
                app_id,
                sleep_milliseconds=random.randint(500, 2000),  # Random delay between requests
                lang='en',  # or 'es', 'de'.. https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes
                country='gb',  # or 'gb', 'de', 'es'.. https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
                sort=Sort.NEWEST,
                filter_score_with=None
            )
            return reviews
        except Exception as e:
            print(f"Error fetching reviews: {e}. Retrying in {delay} milliseconds...")
            time.sleep(delay / 1000)  # Convert milliseconds to seconds
            retries += 1
            delay *= 2  # Exponential backoff

    raise Exception("Maximum retries reached. Failed to fetch reviews.")


# Attempt to fetch reviews with error handling
try:
    reviews = fetch_reviews_with_retry(
        'com.ovz.carscanner')  # app id found in app link play.google.com/store/apps/details?id=
    reviews_df = pd.DataFrame(reviews)
    reviews_df.drop_duplicates(subset=['reviewId'], inplace=True)
    reviews_df.to_csv('scrapes/com.ovz.carscanner_uk.csv', index=False)  # change file name for new scrape
    print(f"Reviews successfully fetched and saved. Total reviews scraped: {len(reviews_df)}")
except Exception as e:
    print(str(e))
