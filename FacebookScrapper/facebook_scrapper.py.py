import requests
import pandas as pd
import time
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
ACCESS_TOKEN = 'YOUR_ACCESS_TOKEN'
GROUP_ID = 'YOUR_GROUP_ID'
BASE_URL = f'https://graph.facebook.com/v17.0/{GROUP_ID}/feed'
FIELDS = 'id,message,created_time,from,comments.limit(1000){from,message},reactions.summary(total_count)'
INITIAL_DELAY = 1
MAX_DELAY = 32
MAX_RETRIES = 5

def get_group_posts(access_token, base_url, fields):
    params = {
        'access_token': access_token,
        'fields': fields,
        'limit': 100  # Adjust based on your needs and API limits
    }
    posts = []
    delay = INITIAL_DELAY
    retries = 0

    while True:
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

            if 'data' in data:
                posts.extend(data['data'])
                logging.info(f"Fetched {len(data['data'])} posts. Total: {len(posts)}")

            if 'paging' in data and 'next' in data['paging']:
                base_url = data['paging']['next']
                params = {}  # Reset params as 'next' URL includes them
                delay = INITIAL_DELAY  # Reset delay on successful request
                retries = 0
            else:
                break

            time.sleep(delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            retries += 1
            if retries > MAX_RETRIES:
                logging.error("Max retries reached. Exiting.")
                break
            delay = min(delay * 2, MAX_DELAY)
            logging.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    return posts

def process_posts(posts):
    processed_data = []
    for post in posts:
        post_data = {
            'post_id': post['id'],
            'message': post.get('message', ''),
            'created_time': datetime.strptime(post['created_time'], '%Y-%m-%dT%H:%M:%S+0000').strftime('%Y-%m-%d %H:%M:%S'),
            'author_name': post['from']['name'],
            'author_id': post['from']['id'],
            'reactions_count': post['reactions']['summary']['total_count'],
            'comments_count': len(post.get('comments', {}).get('data', [])),
        }
        
        comments = post.get('comments', {}).get('data', [])
        commenters = [comment['from']['name'] for comment in comments]
        post_data['commenters'] = ', '.join(commenters)
        
        processed_data.append(post_data)
    
    return processed_data

def main():
    logging.info("Starting to fetch Facebook group posts...")
    posts = get_group_posts(ACCESS_TOKEN, BASE_URL, FIELDS)
    logging.info(f"Total posts fetched: {len(posts)}")

    logging.info("Processing posts...")
    processed_posts = process_posts(posts)

    logging.info("Converting to DataFrame and saving to CSV...")
    df = pd.DataFrame(processed_posts)
    csv_filename = f'facebook_group_posts_{GROUP_ID}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df.to_csv(csv_filename, index=False)

    logging.info(f"Data saved to {csv_filename}")
    logging.info(f"Total posts saved: {len(df)}")

if __name__ == "__main__":
    main()