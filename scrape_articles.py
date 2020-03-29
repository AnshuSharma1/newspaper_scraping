import hashlib
import json
import os
from datetime import datetime
from urllib.parse import urlparse

import newspaper

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

    data = {
        'id': str(get_hash(article.url + article.title + str(article.publish_date))),
        'current_date': datetime.now().strftime('%s'),
        'authors': article.authors,
        'story_date': article.publish_date.date().strftime('%s'),
        'story_time': article.publish_date.time().strftime('%s'),
        'body': article.text,
        'title': article.title,
        'url': article.url,
        'source': get_domain_name(article.url),
        'category': article.tags,
        'topics': article.keywords,
        'summary': article.summary
    }

    return data


def serialize_dict_data(d):
    for key in d:
        if not isinstance(d[key], (str, float, int)):
            d[key] = json.dumps(d[key])


def save_json_file(article_data, article_path):
    filename = article_path + article_data['source'] + '_' + article_data['id'] + '.json'

    with open(filename, 'w') as fp:
        json.dump(article_data, fp)


def main():
    current_date = datetime.now().date()
    output_dir = 'news_articles/' + str(current_date) + '/'
    os.makedirs(output_dir, exist_ok=True)
    news_list = get_file_data(os.path.join(BASE_DIR, 'input.txt'))
    for link in news_list:
        paper = newspaper.build(link.rstrip('\n'), memoize_articles=False)
        print(paper.url)
        newspaper.news_pool.set([paper], threads_per_source=2)
        newspaper.news_pool.join()
        for article in paper.articles:
            article_json = process_article(article)
            story_date = datetime.fromtimestamp(int(article_json['story_date'])).date()
            if story_date == current_date:
                save_json_file(article_json, output_dir)
            serialize_dict_data(article_json)


if __name__ == "__main__":
    main()
