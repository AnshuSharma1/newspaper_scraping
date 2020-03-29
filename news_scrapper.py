import asyncio
import argparse
import hashlib
import json
import os
from datetime import datetime
from urllib.parse import urlparse

import newspaper
import redis

ARTICLE_KEY = 'article:{id}'
ARTICLE_SUMMARY_KEY = 'summary:{source}'
ARTICLE_LIST = 'articles:zset:'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_file_data(file_path):
    with open(file_path, 'r') as f:
        return f.readlines()


async def get_hash(identifier):
    return hashlib.md5(identifier.encode('utf8')).hexdigest()


async def get_domain_name(url):
    result = urlparse(url)
    return result.netloc


async def get_process_article_data(article):
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
    current_date = str(datetime.now().date())
    story_date = article_data['story_date']
    if story_date == current_date:
        output_path = dir_path + '/' + current_date + '/'
        os.makedirs(output_path, exist_ok=True)
        filename = output_path + article_data['source'] + '.json'

        with open(filename, 'w+') as fp:
            if not fp.read(1):
                data = {
                    'results':  [article_data]
                }
                json.dump(data, fp)
            else:
                json_data = json.loads(fp)
                json_data['results'] = json_data['results'] + [article_data]
                json.dump(json_data, fp)


@asyncio.coroutine
def process_and_ingest(redis_con, article, dir_path):
    article_json = yield from get_process_article_data(article)
    if article_json:
        save_json_file(article_json, dir_path)
        article_key = ARTICLE_KEY.format(id=article_json['id'])
        yield from ingest_data(redis_con, article_key, article_json)


def get_redis_connection():
    return redis.Redis().from_url('redis://127.0.0.1:6379/3')


def scrape_articles(dir_name, source_name):
    news_list = get_file_data(os.path.join(BASE_DIR, source_name))
    redis_con = get_redis_connection()
    futures = []
    for link in news_list:
        paper = newspaper.build(link.rstrip('\n'), memoize_articles=False)
        paper.download()
        for article in paper.articles:
            futures.append(process_and_ingest(redis_con, article, dir_name))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(futures))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--root_dir', action='store', required=True)
    parser.add_argument('--source_list', action='store', required=True)
    args = parser.parse_args()

    output_dir = args.root_dir
    source_file = args.source_list
    scrape_articles(output_dir, source_file)
