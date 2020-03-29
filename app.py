from collections import OrderedDict
from datetime import datetime, timedelta
from urllib import parse as url_parse

import redis
from flask import Flask, request, jsonify

from constants import ARTICLE_SUMMARY_KEY, ARTICLE_LIST, REDIS_CONNECTION_URL

app = Flask(__name__)


def get_redis_connection():
    return redis.Redis().from_url(REDIS_CONNECTION_URL, decode_responses=True)


redis_con = get_redis_connection()


@app.route('/')
def root():
    url_list = [
        request.url + 'stats/?source=in.finance.yahoo.com&start_date=25-03-2020&end_date=29-03-2020',
        request.url + 'articles/'
    ]
    return jsonify(url_list)


def _replace_query_param(url, key, val):
    """
    Given a URL and a key/val pair, set or replace an item in the query
    parameters of the URL, and return the new URL.
    """
    result = url_parse.urlparse(url)
    query_dict = url_parse.parse_qs(result.query, keep_blank_values=True)
    query_dict[key] = [val]
    query = url_parse.urlencode(sorted(list(query_dict.items())), doseq=True)
    return url_parse.urlunsplit((result.scheme, result.netloc, result.path, query, result.fragment))


def _get_next_prev_url(url, page_no, count, page_size):
    page_param = 'page_no'
    max_page_no = int(count / page_size) + int(count % page_size != 0)
    next_link = prev_link = None
    if max_page_no > page_no:
        next_link = _replace_query_param(url, page_param, page_no + 1)
    if page_no > 1:
        prev_link = _replace_query_param(url, page_param, page_no - 1)
    return next_link, prev_link


@app.route('/articles/')
def articles():
    """Show paginated articles list for specified page number and size"""
    page_no = int(request.args.get("page_no", 1))
    page_size = int(request.args.get("page_size", 10))
    result_count = redis_con.zcard(ARTICLE_LIST)
    start = (page_no - 1) * page_size
    if start >= result_count:
        return jsonify({
            'status': False,
            'message': 'Invalid page'
        })

    article_list = redis_con.zrange(ARTICLE_LIST, start=start, end=start + page_size - 1)
    next_, prev_ = _get_next_prev_url(url=request.url, page_no=page_no,
                                      count=result_count, page_size=page_size)
    response = OrderedDict([
        ('count', result_count),
        ('next', next_),
        ('previous', prev_),
        ('results', [])
    ])

    for article in article_list:
        response['results'].append(redis_con.hgetall(article))

    return jsonify(response)


@app.route('/stats/')
def get_article_stats():
    """Article statistics : How many articles captured by source on a date"""
    source = request.args.get("source", None)
    start_date = request.args.get("start_date", None)
    end_date = request.args.get("end_date", None)

    if None in [source, start_date]:
        return 'Insufficient args'

    stats_key = ARTICLE_SUMMARY_KEY.format(source=source)
    if not redis_con.exists(stats_key):
        return 'Stats not found'

    try:
        total_count = 0
        start_date = datetime.strptime(start_date, "%d-%m-%Y")
        start_count = redis_con.zscore(stats_key, str(start_date.date()))
        total_count += int(start_count) if start_count else 0
        # If None end date get count for start date
        if end_date is not None:
            end_date = datetime.strptime(end_date, "%d-%m-%Y")
            # Get count between given range
            while end_date > start_date:
                cur_count = redis_con.zscore(stats_key, str(end_date.date()))
                if cur_count:
                    total_count += int(cur_count)
                end_date -= timedelta(days=1)
        return f'{total_count} Articles found'
    except Exception as e:
        raise e


if __name__ == '__main__':
    # Hit http://127.0.0.1:5000/?
    app.run(debug=True)
