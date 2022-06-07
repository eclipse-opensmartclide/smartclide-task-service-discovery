#*******************************************************************************
# Copyright (C) 2022 AIR Institute
# 
# This program and the accompanying materials are made
# available under the terms of the Eclipse Public License 2.0
# which is available at https://www.eclipse.org/legal/epl-2.0/
# 
# SPDX-License-Identifier: EPL-2.0
# 
# Contributors:
#    Adrian Diarte Prieto - initial API and implementation
#*******************************************************************************

import re
import json
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
keywords = []
mutex = threading.Lock()
keywords = get_keywords("keywordsPruebas")


def getRequest(url):
    """
    Create request to param url

    :param url: str
    :return ResulSet:
    """
    headers = {
        'access_token': config.access_token_bitbucket_arr_2
    }
    r = requests.get(url, headers=headers)
    while(r.status_code == 429):  # Error API limit reached
        print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tError 429 {url}")
        time.sleep(10)
        r = requests.get(url, headers=headers)
    return r


def getInfoReposFromKw(kw):
    """
    Create json list with all information from bitbucket search with keyword

    :param kw: str
    :return List:
    """
    global dataErr
    data = []

    # Start keyword & Get max pages
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{kw}  >> \tEMPEZANDO ")
    r = getRequest(f"https://bitbucket.org/repo/all/1?name={kw}")
    soup = BeautifulSoup(r.text, 'lxml')  # Get soup of html
    n = soup.find("section", {"class": "aui-item"})
    n2 = n.find("h1")  # Find title
    rx = re.compile(r'-?\d+(?:\.\d+)?')  # Numbers regex
    numbers = rx.findall(n2.text)  # Find numbers
    limit = (int(numbers[0])//10)+1  # Get max pages

    # Iterate all pages
    for i in range(1, limit+1):

        # Get soup from request & find articles
        r = getRequest(f"https://bitbucket.org/repo/all/{str(i)}?name={kw}")
        soup = BeautifulSoup(r.text, 'lxml')
        articles = soup.find_all("article", {"class": "repo-summary"})
        print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{kw} >> \t{str(i)} / {str(limit)} == {str(len(articles))}")

        # Iterate all repos in page
        for article in articles:
            for a in article.find_all('a', {"class": "repo-link"}, href=True):
                repo = getRequest(f"https://bitbucket.org{a['href']}")

                # Append information repo to json list
                if(repo.status_code == 200):
                    repotext = repo.text.split('window.__initial_state__ = {"section": {"repository": ')
                    if(len(repotext) > 1):
                        datajson = repotext[1].split('}, "global":')[0]
                        # Delete carriage return, spaces, tabulations...
                        datajson = " ".join(re.split(r"\s+", datajson, flags=re.UNICODE))
                        datajson = " ".join(re.split(r"\\n+", datajson, flags=re.UNICODE))
                        datajson = " ".join(re.split(r"\\t+", datajson, flags=re.UNICODE))
                        datajson = " ".join(re.split(r"\\r+", datajson, flags=re.UNICODE))
                        datajson = " ".join(re.split(r"\\r\\n+", datajson, flags=re.UNICODE))
                        data.append(json.loads(datajson))  # Add to list
                    else:
                        # Error repo not found
                        dataErr.append({
                            "link": a['href'],
                            "error": "No info",
                            "keyword": kw
                        })
                else:
                    # Other type of error
                    dataErr.append({
                        "link": a['href'],
                        "error": str(repo.status_code),
                        "keyword": kw
                    })


        # Create security copy csv
        if not data:
            continue
        check_folder(f'bitbucket3_{datetime.now().strftime("%d_%b")}')
        generate_file_del(f'bitbucket3_{datetime.now().strftime("%d_%b")}/bitbucket_{kw.replace(" ","")}_{datetime.now().strftime("%d_%m_%Y")}.csv',data)

    # Return json list
    return data


# Iterate keywords on thread
keywords_split = np.array_split(keywords, 10)
with concurrent.futures.ThreadPoolExecutor(max_workers=len(keywords_split)) as executor:
    for kw in keywords:
        tasks.append(executor.submit(getInfoReposFromKw, kw))

    # Iterate results
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tTratando resultados")
    for result in concurrent.futures.as_completed(tasks):
        data += result.result()

# Generate data file & error file
generate_file_del(f'bitbucket3_{datetime.now().strftime("%d_%m")}.csv', data)
if(dataErr):
    generate_file_del(f'bitbucket3_{datetime.now().strftime("%d_%m")}_err.csv', data)
