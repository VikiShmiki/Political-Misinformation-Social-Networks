import nest_asyncio
import asyncio
from twikit import Client, TooManyRequests
import time
from httpx import Timeout
from datetime import datetime
from random import randint
import csv

nest_asyncio.apply()

timeout = Timeout(60.0, read=60.0)

# Initialize client ONCE
client = Client(language='en-US', timeout=timeout)

async def login_and_save():
    await client.login(auth_info_1='VikiiShmiki', auth_info_2='viktornajdovski99@gmail.com', password='Oliver123@')
    client.save_cookies('cookies.json')
    print("Logged in & cookies saved!")

# asyncio.run(login_and_save())

# Query: original tweets only
QUERY = '(from:SDSMakedonija) -is:retweet'

async def main(client):
    # Load cookies
    client.load_cookies('cookies.json')
    print("Cookies loaded!")

    # Write header row (overwrite file fresh)
    with open('tweets_SDSMakedonija3.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['tweet_count', 'tweet_id', 'tweet.user.screen_name',
                         'tweet.full_text', 'tweet.created_at', 'tweet.retweet_count',
                         'tweet.reply_count', 'tweet.favorite_count', 'tweet.lang',
                         'tweet.user.followers_count', 'tweet.user.following_count', 
                         'tweet.user.verified', 'tweet.user.created_at', 'retweeters'])

    tweet_count = 0

    tweets = await client.search_tweet(QUERY, product='top')
    print('Getting initial tweets...neow')

    if not tweets:
        return

    while tweet_count < 600:
        rows = []

        # Collect rows in batch
        for tweet in tweets:
            tweet_count += 1
            # Try fetching retweeters
            retweeter_usernames = []
            while True:
                try:
                    retweeters = await client.get_retweeters(tweet.id)
                    retweeter_usernames = [user.screen_name for user in retweeters]
                    print(f'Fetched {len(retweeter_usernames)} retweeters')
                    break  # success, exit loop
                except TooManyRequests as e:
                    rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
                    print(f'Rate limit hit! Waiting until {rate_limit_reset}')
                    wait_time = rate_limit_reset - datetime.now()
                    await asyncio.sleep(wait_time.total_seconds())
                except Exception as e:
                    print(f'Error fetching retweeters: {e}')
                    break  # other error, exit


            tweet_data = [
                tweet_count,
                tweet.id,
                tweet.user.screen_name,
                tweet.full_text,
                tweet.created_at,
                tweet.retweet_count,
                tweet.reply_count,
                tweet.favorite_count,
                tweet.lang,
                tweet.user.followers_count,
                tweet.user.following_count,
                tweet.user.verified,
                tweet.user.created_at,
                ','.join(retweeter_usernames)  # Save retweeters as comma-separated
            ]
            rows.append(tweet_data)

        print(f'\nTweet {tweet_count} scraped')

        with open('tweets_SDSMakedonija3.csv', 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
            print(f'{len(rows)} tweets written to CSV.')

        wait_time = randint(5, 10)
        print(f'Waiting {wait_time} seconds...')
        await asyncio.sleep(wait_time)

        try:
            tweets = await tweets.next()
        except TooManyRequests as e:
            rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
            print(f'Waiting until {rate_limit_reset}')
            wait_time = rate_limit_reset - datetime.now()
            await asyncio.sleep(wait_time.total_seconds())
            continue

    print(f'\nScraping done! Total tweets saved: {tweet_count}')

asyncio.run(main(client))
