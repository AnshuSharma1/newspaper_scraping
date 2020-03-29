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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_file_data(file_path):
    with open(file_path, 'r') as f:
        return f.readlines()


def get_hash(identifier):
    return hashlib.md5(identifier.encode('utf8')).hexdigest()


def get_domain_name(url):
    result = urlparse(url)
    return result.netloc


def process_article(article):
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
        'id': str(get_hash(article.title + str(article.publish_date) + authors)),
        'current_date': str(datetime.now()),
        'authors': authors,
        'story_date': str(article.publish_date.date()),
        'story_time': str(article.publish_date.time()),
        'body': article.text,
        'title': article.title,
        'url': article.url,
        'source': get_domain_name(article.url),
        'category': category,
        'topics': topics,
        'summary': article.summary
    }

    return data


def save_json_file(article_list, today, dir_path):
    output_path = dir_path + '/' + today + '/'
    os.makedirs(output_path, exist_ok=True)

    for article_data in article_list:
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


def get_redis_connection():
    return redis.Redis().from_url('redis://127.0.0.1:6379/3')


def scrape_articles(dir_name, source_name):
    current_date = str(datetime.now().date())
    news_list = get_file_data(os.path.join(BASE_DIR, source_name))
    redis_con = get_redis_connection()
    stories_to_save = []
    for link in news_list:
        paper = newspaper.build(link.rstrip('\n'), memoize_articles=False)
        newspaper.news_pool.set([paper], threads_per_source=2)
        newspaper.news_pool.join()
        for article in paper.articles:
            article_json = process_article(article)
            story_date = article_json['story_date']
            if story_date == current_date:
                stories_to_save.append(article_json)
            article_key = ARTICLE_KEY.format(id=article_json['id'])
            if not redis_con.exists(article_key):
                redis_con.hmset(article_key, mapping=article_json)
                redis_con.zincrby(
                    name=ARTICLE_SUMMARY_KEY.format(source=article_json['source']),
                    amount=1,
                    value=article_json['story_date'],
                )

    save_json_file(
        stories_to_save,
        current_date,
        dir_name
    )


if __name__ == "__main__":
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('--root_dir', action='store', required=True)
    my_parser.add_argument('--source_list', action='store', required=True)
    args = my_parser.parse_args()

    output_dir = args.root_dir
    source_file = args.source_list
    scrape_articles(output_dir, source_file)
