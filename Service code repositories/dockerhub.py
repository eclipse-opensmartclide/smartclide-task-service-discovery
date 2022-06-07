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
import requests
import subprocess
import concurrent.futures
import requests_random_user_agent
import numpy as np
from datetime import datetime
from ADP_util import *

# Initial vars
limit = 100
keywords = []
base_url_gh = "https://github.com/"
base_url_dk = "https://hub.docker.com/r/"
keywords = get_keywords("keywordsPruebas")


def getInfoReposFromKw(keyword):
    """
    Create json list with all information from bitbucket search with keyword

    :param keyword: str
    :return List:
    """
    # Initial vars
    data = []

    # Get data from keyword
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{keyword} >> \tEMPEZANDO ")
    process = subprocess.Popen(['docker', 'search', '--limit', str(limit), '--no-trunc', keyword], stdout=subprocess.PIPE, universal_newlines=True)

    # Iterate all repos
    for i in range(1, limit):
        print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{keyword} >> \t{str(i)}")
        try:
            datarepo = process.stdout.readline()
            output = " ".join(re.split("\s+", datarepo, flags=re.UNICODE))

            # Some fix response
            linea = output.split(" ")
            linea.append('')
            linea.append('')
            linea.append('')
            name = linea[0]
            linea[0] = ""

            # If not first line
            if(name != "NAME"):
                rx = re.compile(r'-?\d+(?:\.\d+)?')
                nums = rx.findall(datarepo)
                if(len(nums) != 0):
                    # Get info from line
                    line = datarepo.split(nums[len(nums)-1])
                    if(len(line[1]) == 18):
                        off = "1"
                        aut = "0"
                    elif(len(line[1]) == 19):
                        off = "1"
                        aut = "0"
                    elif(len(line[1]) == 25):
                        aut = "1"
                        off = "0"
                    else:
                        off = "0"
                        aut = "0"

                    linea = " ".join(linea)
                    rx = re.compile(r'-?\d+(?:\.\d+)?')
                    numbers = rx.findall(linea)

                    # Fix response problems
                    if(len(numbers) > 0):
                        stars = numbers[len(numbers)-1]
                        desc = linea.split(numbers[len(numbers)-1])
                        if(len(numbers) > 1):
                            descr = ""
                            for i in range(0, len(desc)):
                                descr += desc[i]
                        else:
                            descr = desc[0]
                        desc.append("")
                    else:
                        stars = 0
                        descr = linea

                    # Try to get github page
                    url = f"{base_url_dk}{name}"
                    url2 = f"{base_url_gh}{name}"
                    if(url != "{ base_url_dk }"):
                        r = requests.get(url2)
                        if(r.status_code == 404 or r.status_code == 403):
                            url2 = "not found"
                        # Put it on object
                        datarepo = {
                            "name": name,
                            "description": descr,
                            "stars": stars,
                            "official": off,
                            "automated": aut,
                            "github": url2,
                            "keyword": keyword
                        }
                        data.append(datarepo)  # Add to list
        except UnicodeDecodeError:
            print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{keyword} >> \tError")

    # Generate file
    check_folder(f'dockerhub_{datetime.now().strftime("%d_%b")}')
    generate_file(f'dockerhub_{datetime.now().strftime("%d_%b")}/dockerhub_{keyword.replace(" ","")}_{datetime.now().strftime("%d_%m_%Y")}.csv',data)
    return data


def getdata(keywords, num_Splits):
    """
    Get all data from keywords

    :param keywords: List
    :param num_Splits: int
    :return List:
    """
    # Initial function vars
    data = []
    tasks = []

    # Split keywords on max_workers
    keywords_split = np.array_split(keywords, num_Splits)

    # Create all threads
    for split in range(len(keywords_split)):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(keywords_split)) as executor:
            for kw in keywords_split[split]:
                tasks.append(executor.submit(getInfoReposFromKw, kw))

    # iterate results
    for result in concurrent.futures.as_completed(tasks):
        if(type(result.result() is list)):
            if(result.result() is not None):
                data += result.result()

    return data


# Get data
data = getdata(keywords, 10)

# Generate file
generate_file(f'dockerhub_{datetime.now().strftime("%d_%m")}.csv', data)
