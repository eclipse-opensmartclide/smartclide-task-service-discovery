import requests, json, re, concurrent.futures
from datetime import datetime
import pandas as pd
import numpy as np
from config import config

# Get info for repo url with api
dataglobal = []
def getrepos(keyword):
    global dataglobal
    data
    page = 1
    ctrl = True
    while(ctrl):
        print(str(page) +" / "+keyword)
        url = f"https://gitlab.com/api/v4/search?scope=projects&search={keyword}&per_page=100&page={page}"
        headers = {
            'Authorization': config.access_token_gitlab
        }
        response = requests.request("GET", url, headers=headers)
        headers = response.headers
        respjson = response.json()
        for repo in respjson:
            if not isinstance(repo,dict):
                print(repo)
                continue
            if('description' not in repo):
                repo['description'] = ""
            elif(repo['description'] is not None):
                desc = " ".join(re.split("\s+", repo['description']))
            else:
                desc =""
            if('id' not in repo): repo['id']= ""
            if('name' not in repo): repo['name']=""
            if('name_with_namespace' not in repo): repo['name_with_namespace']=""
            if('path' not in repo): repo['path']=""
            if('path_with_namespace' not in repo): repo['path_with_namespace']=""
            if('created_at' not in repo): repo['created_at']=""
            if('default_branch' not in repo): repo['default_branch']=""
            if('tag_list' not in repo): repo['tag_list']=""
            if('ssh_url_to_repo' not in repo): repo['ssh_url_to_repo']=""
            if('http_url_to_repo' not in repo): repo['http_url_to_repo']=""
            if('web_url' not in repo): repo['web_url']=""
            if('readme_url' not in repo): repo['readme_url']=""
            if('avatar_url' not in repo): repo['avatar_url']=""
            if('forks_count' not in repo): repo['forks_count']=""
            if('star_count' not in repo): repo['star_count']=""
            if('last_activity_at' not in repo): repo['last_activity_at']=""

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

            dataglobal.append(datarepo)
            data.append(datarepo)

        # Check next pages
        if('X-Next-Page' not in headers):
            ctrl = False
        else:
            if(headers['X-Next-Page']):
                page += 1
            else:
                ctrl = False

    df = pd.json_normalize(data=data)
    df.to_csv(r'C:/Datos/Repos/pruebas/docker/outputs/output_gitlab_'+keyword+"_" + datetime.now().strftime('%d_%m_%Y') + '.csv', index = False)
    return data

# Get keywords
f = open("keywordsAll.txt")
keywords = []
for line in f:
    keywords.append(line.rstrip('\n'))
f.close()

# Iterate keywordss
data = []
tasks = []
with concurrent.futures.ThreadPoolExecutor(max_workers = 16) as executor:
    for kw in keywords:
        print(kw)
        tasks.append(executor.submit(getrepos, kw))

    # iterate results
    for result in tasks:
        data+=result.result()
        df = pd.json_normalize(data=data)
        df.to_csv('output_gitlab.csv', index = False)