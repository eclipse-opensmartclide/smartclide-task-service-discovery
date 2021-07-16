#
# Copyright (c) 2021 AIR Institute - Adrian Diarte Prieto
#
# This file is part of smartclide
# (see https://smartclide.eu/).
#
# This program is distributed under Eclipse Public License 2.0
# (see https://github.com/adriandpdev/Smartclide_apitest/blob/main/LICENSE.md)
#

import re
import time
import requests
import threading
import concurrent.futures
import requests_random_user_agent
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
from ADP_config import config
from ADP_util import *

# Initial vars
data = []
tasks = []
dataErr = []
mutex = threading.Lock()
keywords = get_keywords("keywordsPruebas")


def getReposKw(kw):
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{kw} >> \tEMPEZANDO ")

    # Initial vars
    global dataErr
    data = []
    base_url = f"https://bitbucket.org/repo/all/1?name={kw}"
    headers = {'access_token': config.access_token_bitbucket_arr_1}

    # Get request & find max pages
    r = requests.get(base_url, headers=headers)
    while(r.status_code == 429):  # Error API limit reached
        print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tError 429 - F1")
        time.sleep(60)
        r = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')  # Get soup of html
    n = soup.find("section", {"class": "aui-item"})
    n2 = n.find("h1")  # Find title
    rx = re.compile(r'-?\d+(?:\.\d+)?')  # Numbers regex
    numbers = rx.findall(n2.text)  # Find numbers
    limit = (int(numbers[0])//10)+1  # Get max pages

    # Iterate all pages
    for i in range(1, limit+1):

        # Get requests
        url = f"https://bitbucket.org/repo/all/{str(i)}?name={kw}"
        headers = {
            'access_token': config.access_token_bitbucket_arr_1
        }
        r = requests.get(url, headers=headers)
        while(r.status_code == 429):  # Error API limit reached
            print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tError 429 - F2")
            time.sleep(60)
            r = requests.get(url, headers=headers)

        # Get soup from request & find articles
        soup = BeautifulSoup(r.text, 'lxml')
        articles = soup.find_all("article", {"class": "repo-summary"})
        print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{kw} >> \t{str(i)} / {str(limit)} == {str(len(articles))}")
        for article in articles:
            # Get info from article
            info = article.find('ul', {"class": "repo-metadata clearfix"})
            info = info.find_all('li')
            try:
                desc = article.find('p').text,
            except AttributeError:
                desc = ""
                dataErr.append({
                    "full_name": article.find('a', {"class": "repo-link"}, href=True).text,
                    "Error": "AttributeError"
                })

            # Put it on object
            datarepo = {
                "full_name": article.find('a', {"class": "repo-link"}, href=True).text,
                "description": desc,
                "link": article.find('a', {"class": "repo-link"}, href=True)['href'],
                "updated_on": info[1].find('time')['datetime'],
                "watchers": " ".join(re.split(r"\n+", info[0].find('a').text.strip(), flags=re.UNICODE)),
                "keyword": kw
            }
            data.append(datarepo)  # Add to list

        # Check folder & create csv
        check_folder(f'bitbucket_{datetime.now().strftime("%d_%b")}')
        generate_file(f'bitbucket_{datetime.now().strftime("%d_%b")}/bitbucket_{datetime.now().strftime("%d_%m")}_resum.csv', data)

    return data


# Iterate keywords on threads
keywords_split = np.array_split(keywords, 10)
with concurrent.futures.ThreadPoolExecutor(max_workers=len(keywords_split)) as executor:
    for kw in keywords:
        tasks.append(executor.submit(getReposKw, kw))

    # Iterate results
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tTratando resultados")
    for result in concurrent.futures.as_completed(tasks):
        data += result.result()

# Generate data file & error files
generate_file(f'bitbucket_{datetime.now().strftime("%d_%m")}_resum.csv', data)
if(dataErr):
    generate_file(f'bitbucket_{datetime.now().strftime("%d_%m")}_resum_err.csv', dataErr)
