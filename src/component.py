'''
Extract media monitoring from Monitora

The subscription of media monitoring is required in order
to obtain a valid token.
'''
import csv
import logging
import sys
import requests
import json

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# Monitora API
BASE_URL = "https://api.monitora.cz"
BASE_URL_READ_FEED = "%s/feed" % (BASE_URL)

MONITORA_FIELDS = [
    'id', 'title', 'url',
    'news_source_id', 'news_source_name', 'news_source_publisher_id',
    'news_source_publisher_name', 'news_source_publisher_country',
    'news_source_publisher_reg_no', 'news_source_publisher_street',
    'news_source_publisher_municipality', 'news_source_publisher_postal_code',
    'news_source_category_id', 'news_source_category_name',
    'news_source_category_category_type_id', 'news_source_category_category_type_text',
    'news_source_category_category_type_shorttext',
    'news_source_category_category_type_label',
    'news_source_category_category_type_color',
    'news_source_category_category_type_fill_color', 'news_source_country',
    'news_source_country_code', 'news_source_language', 'news_source_url',
    'news_source_favicon_url', 'news_source_monthly_sessions',
    'news_source_monthly_ru', 'news_source_daily_ru', 'publication_frequency',
    'press_amount', 'readership', 'sold_amount', 'print_ad_price_full_page',
    'listenership', 'identical_articles', 'pdf_url',
    'small_image_url', 'big_image_url', 'published', 'authors', 'perex',
    'language', 'word_count', 'text_relevance', 'social_shares', 'pages',
    'all_pages', 'cover_page', 'imprint_page', 'prev_page', 'next_page',
    'issue', 'reach', 'GRP', 'OTS', 'AVE', 'topic_monitor_id', 'topic_monitor_name',
    'topic_monitor_image_url', 'keyword_monitors', 'article_tags', 'note',
    'discussion_thread_id', 'text', 'monthly_ru', 'publisher', 'daily_ru',
    'monthly_sessions', 'sentiment_text'
]

# Configuration variables
KEY_API_TOKEN = '#api_token'
KEY_FEED_ID = 'feed_id'

STATE_LAST_ID = 'LAST_ID'

REQUIRED_PARAMETERS = [KEY_API_TOKEN, KEY_FEED_ID]
REQUIRED_IMAGE_PARS = []


def get_request(url, authorization_token, params=None):
    return requests.request(
        "GET",
        url,
        headers={
            "Authorization": "Token %s" % (authorization_token),
            "Content-Type": "application/json; charset=utf-8"
        },
        params=params
    )


def read_feed(api_token, topic_monitor_id, last_id):
    """ Get all data from feed that are not older than X """
    articles = []
    next_page = None
    logging.info("Downloading articles starting at %s" % (last_id + 1))

    response = get_request(
        "%s/%s" % (BASE_URL_READ_FEED, topic_monitor_id),
        api_token,
        {
            'lower_id': last_id + 1
        }
    )

    if response.status_code == 401:
        print("Authentication Failed")
        sys.exit(1)
    elif response.status_code == 404:
        print("Invalid Feed ID")
        sys.exit(1)

    while True:
        new_data = json.loads(response.content)
        new_articles = new_data['articles']

        articles += new_articles

        logging.info("Downloading page with %d results" % (len(new_articles)))

        if not new_data['next_url']:
            break

        next_page = new_data['next_url']

        response = get_request(
            next_page,
            api_token
        )

        if response.status_code != 200:
            print(response.status_code, file=sys.stderr)
            print(response.text, file=sys.stderr)
            raise ConnectionError()

    return articles


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        if len(self.configuration.tables_output_mapping) != 1:
            logging.error("Output table mapping with one entry is required")
            sys.exit(1)

        result_filename = self.configuration.tables_output_mapping[0]['source']
        last_processed_id = int(self.get_state_file().get(STATE_LAST_ID, "0"))

        articles = read_feed(
            params.get(KEY_API_TOKEN),
            params.get(KEY_FEED_ID),
            last_processed_id
        )

        table = self.create_out_table_definition(result_filename, incremental=True, primary_key=['id'])

        def _flatten_dict(dictionary, prefix):
            flatten = {}
            for k in dictionary.keys():
                if prefix != '':
                    merged_key = "%s_%s" % (prefix, k)
                else:
                    merged_key = k

                if dictionary[k] is None:
                    flatten[k] = ""
                elif type(dictionary[k]) is dict:
                    flatten.update(_flatten_dict(dictionary[k], merged_key))
                elif type(dictionary[k]) is str or type(dictionary[k]) is int or type(dictionary[k]) is float:
                    flatten[merged_key] = dictionary[k]
                elif type(dictionary[k]) is list:
                    flatten[k] = str(dictionary[k])
                else:
                    logging.error("Invalid type encountered '%s'" % (type(a[k])))
                    sys.exit(2)
            return flatten

        flatten_articles = []
        for a in articles:
            flatten = _flatten_dict(a, '')
            flatten_articles.append(flatten)

        with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=MONITORA_FIELDS, dialect='kbc')
            writer.writeheader()

            max_id = last_processed_id
            for a in flatten_articles:
                writer.writerow(dict([(k, a.get(k, '')) for k in MONITORA_FIELDS]))
                max_id = max(max_id, a['id'])

        self.write_manifest(table)
        logging.info("Last article ID was %s" % (max_id))
        self.write_state_file({STATE_LAST_ID: max_id})


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
