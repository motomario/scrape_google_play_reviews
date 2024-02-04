#SSL errors

from google_play_scraper import Sort, reviews_all
import csv
import time
import random
import json
import ssl
import requests

# Specify the paths to the self-signed certificate and private key files
certificate_file = '/Users/marius/PycharmProjects/scrape/certificate.pem'  # Replace with the actual path to your certificate.pem file
private_key_file = '/Users/marius/PycharmProjects/scrape/private-key.pem'  # Replace with the actual path to your private-key.pem file

# Create an SSL context with the certificate and private key
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ssl_context.load_cert_chain(certfile=certificate_file, keyfile=private_key_file)

# Use the SSL context for your requests


url = 'https://play.google.com'  # Replace with the URL you want to access
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}  # Replace with appropriate headers
timeout = 10  # Replace with your desired timeout value

# Make a request using the SSL context
response = requests.get(url, headers=headers, timeout=timeout, verify=False, cert=(certificate_file, private_key_file))

# Verify the response
if response.status_code == 200:
    print('Request was successful')
else:
    print('Request failed')

# Close the response
response.close()


def save_checkpoint(app_id, fetched):
    """
    Saves the checkpoint data, including the number of fetched reviews.
    """
    checkpoint_data = {'fetched': fetched}
    with open(f"{app_id}_checkpoint.json", 'w') as f:
        json.dump(checkpoint_data, f)


def load_checkpoint(app_id):
    """
    Loads the checkpoint data to resume fetching from the last state.
    """
    try:
        with open(f"{app_id}_checkpoint.json", 'r') as f:
            checkpoint_data = json.load(f)
            return checkpoint_data.get('fetched', 0)
    except FileNotFoundError:
        return 0


def load_existing_review_ids(filename):
    """
    Loads existing review IDs from a CSV file to avoid duplicates.
    """
    try:
        with open(filename, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader, None)  # Skip header
            return {row[0] for row in reader}
    except FileNotFoundError:
        return set()


def simulate_human_pause():
    """
    Simulates human-like pauses at random intervals.
    """
    if random.randint(1, 10) > 9:  # 10% chance of a longer pause
        time.sleep(random.uniform(1, 3))  # Random pause


def fetch_reviews(app_id, lang='en', sort=Sort.NEWEST, total_count=200, pause=2.0,
                  filename="obdeleven_reviews.csv"):
    """
    Fetches reviews for the app, with checkpointing and duplicate checking.
    """
    fetched = load_checkpoint(app_id)
    existing_review_ids = load_existing_review_ids(filename)
    all_reviews = []

    while fetched < total_count:
        try:
            batch_size = 200
            batch_count = min(batch_size, total_count - fetched)
            print(f"Fetching {batch_count} reviews...")
            reviews = reviews_all(
                app_id,
                sleep_milliseconds=int(1000 * random.uniform(pause, pause * 2)),
                lang=lang,
                sort=sort,
                count=batch_count
            )
            # Filter out duplicates before adding to all_reviews
            new_reviews = [review for review in reviews if review['reviewId'] not in existing_review_ids]
            all_reviews.extend(new_reviews)
            fetched += len(new_reviews)
            save_checkpoint(app_id, fetched)
            existing_review_ids.update([review['reviewId'] for review in new_reviews])
            print(f"Fetched {fetched} reviews so far.")
            simulate_human_pause()
        except Exception as e:
            print(f"Error fetching reviews: {e}. Pausing before retry...")
            time.sleep(pause * 5)  # Longer pause on error

    return all_reviews


def write_reviews_to_csv(reviews, filename):
    """
    Writes reviews to a CSV file, avoiding duplicates.
    """
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for review in reviews:
            writer.writerow([review['reviewId'], review['userName'], review['score'], review['at'], review['content'],
                             review.get('reviewCreatedVersion', 'N/A')])


if __name__ == "__main__":
    app_id = "com.voltasit.obdeleven"
    total_reviews_to_fetch = 200
    reviews_filename = "obdeleven_reviews.csv"

    # Check if the CSV exists to add the header
    if not load_existing_review_ids(reviews_filename):
        with open(reviews_filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["ReviewId", "Username", "Rating", "Date", "Content", "AppVersion"])

    reviews = fetch_reviews(app_id, total_count=total_reviews_to_fetch, filename=reviews_filename)
    if reviews:
        write_reviews_to_csv(reviews, reviews_filename)
        print("Finished writing reviews to CSV.")
