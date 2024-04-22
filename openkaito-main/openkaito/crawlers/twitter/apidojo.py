from datetime import datetime
from apify_client import ApifyClient

import bittensor as bt

from openkaito.evaluation.utils import tweet_url_to_id

import requests
import json
from datetime import datetime
from apify_client import ApifyClient
from itertools import islice

class ApiDojoTwitterCrawler:
    def __init__(self, api_key, timeout_secs=80):
        self.client = ApifyClient(api_key)

        self.timeout_secs = timeout_secs

        self.actor_id = "apidojo/tweet-scraper"

    def get_tweets_by_urls(self, urls: list):
        """
        Get tweets by urls.

        Args:
            urls (list): The urls to get tweets from.

        Returns:
            list: The list of tweet details.
        """

        params = {
            "startUrls": urls[:10],
            "maxItems": 10,
            "maxTweetsPerQuery": 1,
            "onlyImage": False,
            "onlyQuote": False,
            "onlyTwitterBlue": False,
            "onlyVerifiedUsers": False,
            "onlyVideo": False,
        }

        run = self.client.actor(self.actor_id).call(
            run_input=params, timeout_secs=self.timeout_secs
        )
        first_ten_items = islice(self.client.dataset(run["defaultDatasetId"]).iterate_items(), 10)
        return self.process_list(
            list(first_ten_items)
        )

    def get_tweets_by_ids_with_retries(self, ids: list, retries=1):
        """
        Get tweets by tweet ids with retries.

        Args:
            ids (list): The tweet ids to get tweets from.
            retries (int): The number of retries to make.

        Returns:
            dict: The dict of tweet id to tweet details.
        """
        result = {}
        remaining_ids = ids[:10]
        for id in remaining_ids:
            bt.logging.info(f"REMAINING IDS: {id}")
        while retries > 0 and len(remaining_ids) > 0:
            bt.logging.debug(f"Trying fetching ids: {remaining_ids}")
            urls = [f"https://x.com/x/status/{id}" for id in remaining_ids]
            tweets = self.get_tweets_by_urls(list(urls))
            for tweet in tweets:
                result[tweet["id"]] = tweet
            remaining_ids = ids[10:20] if len(ids) > 10 else []
            retries = 0

        return result

    def fetch_tweets(self, username: list):
        params = {
            "maxItems": 5,
            "onlyImage": False,
            "onlyQuote": False,
            "onlyTwitterBlue": False,
            "onlyVerifiedUsers": False,
            "onlyVideo": False,
            "twitterHandles": [username]
        }
        run = self.client.actor(self.actor_id).call(run_input=params, timeout_secs=self.timeout_secs)
        items = self.client.dataset(run["defaultDatasetId"]).iterate_items()
        return list(islice(items, 5))

    def search(self, query: str, author_usernames: list = None, max_size: int = 10, results: list = []):
        """
        Searches for the given query on the crawled data.

        Args:
            query (str): The query to search for.
            max_size (int): The max number of results to return.

        Returns:
            list: The list of results.
        """
        # bt.logging.debug(
        #     f"Crawling for query: '{query}', authors: {author_usernames} with size {max_size}"
        # )

        results = []
        for username in author_usernames:
            user_tweets = self.fetch_tweets(username)
            results.extend(user_tweets[:5])

        if len(results) > max_size:
            results = results[:max_size]

        # if author_usernames:
        #     for username in author_usernames:
        #         user_tweets = self.fetch_tweets(username)
        #         results.extend(user_tweets[:5])
        # else:
        #     for username in query.author_usernames:
        #         user_tweets = self.fetch_tweets(username)
        #         limited_tweets = user_tweets[:max_size]
        #         results.extend(limited_tweets)

        result = self.process_list(results)
        bt.logging.info(f"Apify Actor Result: {result}")
        return result

    def process_item(self, item):
        """
        Process the item.

        Args:
            item (dict): The item to process.

        Returns:
            dict: The processed item.
        """
        time_format = "%a %b %d %H:%M:%S %z %Y"
        return {
            "id": item["id"],
            "url": item["url"],
            "username": item["author"]["userName"],
            "text": item.get("text"),
            "created_at": datetime.strptime(
                item.get("createdAt"), time_format
            ).isoformat(),
            "quote_count": item.get("quoteCount"),
            "reply_count": item.get("replyCount"),
            "retweet_count": item.get("retweetCount"),
            "favorite_count": item.get("likeCount"),
        }

    def process_list(self, results):
        """
        Process the results from the search.

        Args:
            results (list): The list of results to process.

        Returns:
            list: The list of processed results.
        """
        return [self.process_item(result) for result in results if result.get("id")]


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()
    crawler = ApiDojoTwitterCrawler(os.environ["APIFY_API_KEY"])

    # r = crawler.search("BTC", 5)

    r = crawler.get_tweets_by_ids_with_retries(
        [
            "1762448211875422690",
            "1762389336858022132",
            "1759369749887332577",
            "1760504129485705598",
            "xxxx",
        ],
        retries=2,
    )

    print(r)
