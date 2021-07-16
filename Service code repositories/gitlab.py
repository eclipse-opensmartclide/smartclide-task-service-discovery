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
import requests
import concurrent.futures
import requests_random_user_agent
from ADP_config import config
from datetime import datetime
from ADP_util import *

# Initial vars
data = []
tasks = []
keywords = get_keywords("keywordsPruebas")


def getInfoReposFromKw(keyword):
    """
    Create json list with all information from bitbucket search with keyword

    :param keyword: str
    :return List:
    """
    # Initial function vars
    page = 1
    data = []
    ctrl = True
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{keyword} >> \tEMPEZANDO ")

    # Iterate all pages
    while(ctrl):

        # Take information from page
        print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{keyword} >> \t{str(page)}")
        url = f"https://gitlab.com/api/v4/search?scope=projects&search={keyword}&per_page=100&page={page}"
        headers = {
            'Authorization': config.access_token_gitlab
        }
        response = requests.request("GET", url, headers=headers)
        headers = response.headers
        respjson = response.json()

        # Iterate all repos in jsons
        for repo in respjson:

            # Some response fix
            if not isinstance(repo, dict):
                print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \t{keyword} >> \t{repo}")
                continue
            if('description' not in repo):
                desc = ""
            elif(repo['description'] is not None):
                desc = " ".join(re.split("\s+", repo['description']))
            else:
                desc = ""
            if('id' not in repo): repo['id'] = ""
            if('name' not in repo): repo['name'] = ""
            if('path' not in repo): repo['path'] = ""
            if('web_url' not in repo): repo['web_url'] = ""
            if('tag_list' not in repo): repo['tag_list'] = ""
            if('created_at' not in repo): repo['created_at'] = ""
            if('readme_url' not in repo): repo['readme_url'] = ""
            if('avatar_url' not in repo): repo['avatar_url'] = ""
            if('star_count' not in repo): repo['star_count'] = ""
            if('forks_count' not in repo): repo['forks_count'] = ""
            if('default_branch' not in repo): repo['default_branch'] = ""
            if('ssh_url_to_repo' not in repo): repo['ssh_url_to_repo'] = ""
            if('http_url_to_repo' not in repo): repo['http_url_to_repo'] = ""
            if('last_activity_at' not in repo): repo['last_activity_at'] = ""
            if('name_with_namespace' not in repo): repo['name_with_namespace'] = ""
            if('path_with_namespace' not in repo): repo['path_with_namespace'] = ""

            # Create json repo
            datarepo = {
                "id": repo['id'],
                "description": desc,
                "full_name": repo['name'],
                "name_with_namespace": repo['name_with_namespace'],
                "path": repo['path'],
                "path_with_namespace": repo['path_with_namespace'],
                "created_at": repo['created_at'],
                "default_branch": repo['default_branch'],
                "tag_list": repo['tag_list'],
                "ssh_url_to_repo": repo['ssh_url_to_repo'],
                "http_url_to_repo": repo['http_url_to_repo'],
                "web_url": repo['web_url'],
                "readme_url": repo['readme_url'],
                "avatar_url": repo['avatar_url'],
                "forks_count": repo['forks_count'],
                "star_count": repo['star_count'],
                "last_activity_at": repo['last_activity_at'],
                "keyword": keyword
            }

            # Add json repo to json list
            data.append(datarepo)

        # Check next pages
        if('X-Next-Page' not in headers):
            ctrl = False
        else:
            if(headers['X-Next-Page']):
                page += 1
            else:
                ctrl = False

    # Create dataframe from json list & create security copy csv
    check_folder(f'gitlab_{datetime.now().strftime("%d_%b")}')
    generate_file(f'gitlab_{datetime.now().strftime("%d_%b")}/gitlab_{keyword.replace(" ","")}_{datetime.now().strftime("%d_%m_%Y")}.csv', data)
    return data


# Iterate keywords on thread
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    for kw in keywords:
        tasks.append(executor.submit(getInfoReposFromKw, kw))

    # Iterate results
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tTratando resultados")
    for result in concurrent.futures.as_completed(tasks):
        data += result.result()

# Generate file
generate_file(f'gitlab_{datetime.now().strftime("%d_%m")}.csv', data)
