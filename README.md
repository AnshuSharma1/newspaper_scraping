# Newspaper Scraper - REST API

Goal - The objective is to design and develop a working prototype of centralized news feed system.

## Some of the features of this API/project are :~

* Async, await for asynchronous task execution

* Used Flask for making REST APIs

* Added pagination to show results

* Redis to store article data and other useful stats

## How to setup and run this api

* Run pipenv install & pip shell to install dependencies and activate virtual environment

* Run `news_feed.sh` to run news_scrapper file to scrap articles from passed input file and store them in an output 
directory as well as in redis  

* Run `news_server.sh` to run flask server where you can check statistic parameters category wise

* Once the server starts go to `http://127.0.0.1:5000/<endpoint>` and the endpoint you wanna check.
 - Can check datewise count of articles for a particular news website as well as in a date range (`/stats/`)
 - Can check articles pagewise by specified page size (`/articles/`)