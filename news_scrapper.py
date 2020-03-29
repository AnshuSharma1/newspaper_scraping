import argparse
import asyncio
import hashlib
import json
import os
from datetime import datetime
from urllib.parse import urlparse

import newspaper
import redis

from constants import ARTICLE_SUMMARY_KEY, ARTICLE_LIST, ARTICLE_KEY, REDIS_CONNECTION_URL

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_file_data(file_path):
    with open(file_path, 'r') as f:
        return f.readlines()


async def get_hash(identifier):
    """Unique identifier on basis of title, publish_date and topics"""
    return hashlib.md5(identifier.encode('utf8')).hexdigest()


async def get_domain_name(url):
    result = urlparse(url)
    return result.netloc


async def get_processed_article_data(article):
    """
    Fetches article data from the website, parse and does nlp and returns as json
    :param article: Article object of the website
    :return: JSON response of article data
    """
    try:
        article.download()
        article.parse()
        article.nlp()
    except newspaper.article.ArticleException:
        return

    if article.publish_date is None:
        article.publish_date = datetime.now()

    authors = '|'.join(article.authors)
    category = '|'.join(list(article.tags))
    topics = '|'.join(article.keywords)

    data = {
        'id': str(await get_hash(article.title + str(article.publish_date) + authors)),
        'current_date': str(datetime.now()),
        'authors': authors,
        'story_date': str(article.publish_date.date()),
        'story_time': str(article.publish_date.time()),
        'body': article.text,
        'title': article.title,
        'url': article.url,
        'source': await get_domain_name(article.url),
        'category': category,
        'topics': topics,
        'summary': article.summary
    }

    return data


async def ingest_data(redis_con, article_key, data):
    """Sequentially storing article data and increment the count date wise in redis"""
    if not redis_con.exists(article_key):
        redis_con.hmset(article_key, mapping=data)
        redis_con.zincrby(
            name=ARTICLE_SUMMARY_KEY.format(source=data['source']),
            amount=1,
            value=data['story_date'],
        )
        redis_con.zadd(
            name=ARTICLE_LIST,
            mapping={
                article_key: datetime.now().strftime('%s')
            }
        )


def save_json_file(article_data, dir_path):
    """
    Save json data of an articles w.r.t it's source and story date
    :param article_data: JSON of article data
    :param dir_path: Output dir to store
    """
    current_date = str(datetime.now().date())
    output_path = os.path.join(dir_path, current_date)
    os.makedirs(output_path, exist_ok=True)
    filepath = os.path.join(output_path, article_data['source'] + '.json')
    if not os.path.isfile(filepath):
        with open(filepath, 'w') as fp:
            json_data = {
                'results': {
                    article_data['id']: article_data
                }
            }
            json.dump(json_data, fp)
    else:
        with open(filepath, 'r') as fp:
            json_data = json.load(fp)
            json_data['results'][article_data['id']] = article_data

        with open(filepath, 'w') as fp2:
            json.dump(json_data, fp2)


async def process_and_ingest(redis_con, article):
    """Process data for a single article, save to dir and redis"""
    article_data = await get_processed_article_data(article)
    if article_data:
        article_key = ARTICLE_KEY.format(id=article_data['id'])
        await ingest_data(redis_con, article_key, article_data)

    return article_data


def get_redis_connection():
    return redis.Redis().from_url(REDIS_CONNECTION_URL)


def scrape_articles(dir_name, source_file):
    """
    Scrape newspaper articles from dir_name
    :param dir_name: Output directory to save json files to
    :param source_file: Source file containing links of newspaper websites
    """
    news_list = get_file_data(os.path.join(BASE_DIR, source_file))
    redis_con = get_redis_connection()
    coroutines = []
    for link in news_list:
        paper = newspaper.build(link.rstrip('\n'), memoize_articles=False)
        paper.download()
        for article in paper.articles:
            coroutines.append(process_and_ingest(redis_con, article))
    loop = asyncio.get_event_loop()
    tasks, _ = loop.run_until_complete(asyncio.wait(coroutines))
    for task in tasks:
        save_json_file(task.result(), dir_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--root_dir', action='store', required=True)
    parser.add_argument('--source_list', action='store', required=True)
    args = parser.parse_args()

    output_dir = args.root_dir
    newspapers_file = args.source_list
    scrape_articles(output_dir, newspapers_file)
