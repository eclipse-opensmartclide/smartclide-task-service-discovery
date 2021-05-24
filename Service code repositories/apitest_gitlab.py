import requests, json, re, concurrent.futures, requests_random_user_agent
import pandas as pd
import numpy as np
from config import config
from datetime import datetime

# Initial vars
data = []
tasks = []
keywords = []

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
    print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \t"+keyword +" >> \tEMPEZANDO ")

    # Iterate all pages
    while(ctrl):

        # Take information from page
        print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \t"+kw + " >> \t"+str(page))
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
            if not isinstance(repo,dict):
                print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \t"+kw + " >> \t"+repo)
                continue
            if('description' not in repo):
                desc = ""
            elif(repo['description'] is not None):
                desc = " ".join(re.split("\s+", repo['description']))
            else:
                desc =""
            if('id' not in repo): repo['id']= ""
            if('name' not in repo): repo['name']=""
            if('path' not in repo): repo['path']=""
            if('web_url' not in repo): repo['web_url']=""
            if('tag_list' not in repo): repo['tag_list']=""
            if('created_at' not in repo): repo['created_at']=""
            if('readme_url' not in repo): repo['readme_url']=""
            if('avatar_url' not in repo): repo['avatar_url']=""
            if('star_count' not in repo): repo['star_count']=""
            if('forks_count' not in repo): repo['forks_count']=""
            if('default_branch' not in repo): repo['default_branch']=""
            if('ssh_url_to_repo' not in repo): repo['ssh_url_to_repo']=""
            if('http_url_to_repo' not in repo): repo['http_url_to_repo']=""
            if('last_activity_at' not in repo): repo['last_activity_at']=""
            if('name_with_namespace' not in repo): repo['name_with_namespace']=""
            if('path_with_namespace' not in repo): repo['path_with_namespace']=""

            # Create json repo
            datarepo = {
                "id": repo['id'],
                "description": desc ,
                "full_name": repo['name'] ,
                "name_with_namespace": repo['name_with_namespace'] ,
                "path": repo['path'] ,
                "path_with_namespace": repo['path_with_namespace'] ,
                "created_at": repo['created_at'] ,
                "default_branch": repo['default_branch'] ,
                "tag_list": repo['tag_list'] ,
                "ssh_url_to_repo": repo['ssh_url_to_repo'] ,
                "http_url_to_repo": repo['http_url_to_repo'] ,
                "web_url": repo['web_url'] ,
                "readme_url": repo['readme_url'] ,
                "avatar_url": repo['avatar_url'] ,
                "forks_count": repo['forks_count'] ,
                "star_count": repo['star_count'] ,
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
    df = pd.json_normalize(data=data)
    df.to_csv('outputs/gitlab/gitlab2_'+keyword.replace(" ","")+"_" + datetime.now().strftime('%d_%m_%Y') + '.csv', index = False, encoding='utf-8-sig')
    return data

# Take all keywords from file
f = open("keywords.txt")
for line in f:
    keywords.append(line.rstrip('\n'))
f.close()

# Iterate keywords on thread
with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
    for kw in keywords:
        tasks.append(executor.submit(getInfoReposFromKw, kw))

# Iterate results
print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tTratando resultados")
for result in tasks:
    data+=result.result()

# Generate file
print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tGenerando fichero")
df = pd.json_normalize(data=data)
df.to_csv('outputs/gitlab2_'+ datetime.now().strftime('%d_%m') +'.csv', index = False, encoding='utf-8-sig')
print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tListo! ")