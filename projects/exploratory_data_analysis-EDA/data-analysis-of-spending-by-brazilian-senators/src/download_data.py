import datetime
import re
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import urlopen

import requests
from bs4 import BeautifulSoup


def get_date_today():
    now = datetime.datetime.now()
    return str(now.strftime("%Y-%m-%d"))


def get_links_csv(url):
    """
    Function to download links csv in page
    """
    print('Try analysing page ...')
    links_pg = []

    try:
        # Testing connection
        requests.get(url, timeout=5)

        html = urlopen(url)

        # Create a parser instance to navigate
        page = BeautifulSoup(html, "html.parser")

        # find in tree HTML
        for link in page.find_all('a',
                                  attrs={'href': re.compile("csv")}):
            links_pg.append(link.attrs['href'])
            print(link.attrs['href'])

        return links_pg

    except HTTPError as e:
        print('HTTP error', e)
    except URLError as e:
        print('Server not found, ', e)


def download_file_csv(url):
    """
    Create file data.csv
    """
    list_links_csv = get_links_csv(url)

    # Serialize csv in data/
    data = 'data/raw/'

    for link in list_links_csv:
        # last string
        file_name = link.split('/')[-1]

        # create response object
        r = requests.get(link, stream=True)

        # download started
        with open(data + file_name, 'wb') as f:
            for chunk in r.iter_content():
                f.write(chunk)
        print(f"{data + file_name} downloaded!")
