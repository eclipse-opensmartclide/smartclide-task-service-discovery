import subprocess, requests, json, re, csv, threading, requests, time
from bs4 import BeautifulSoup
import pandas as pd

#Get repositories
keywords = 'service'#, api'
page = 47
ctrl = True
with open('repos_gitlab_example.csv', 'w', newline='',encoding="utf-8") as csvfile:
    spamwriter = csv.writer(csvfile)
    spamwriter.writerow(['id'] + ['description'] + ['name'] + ['name_with_namespace'] + ['path'] + ['path_with_namespace'] + ['created_at'] + ['default_branch'] + ['tag_list'] + ['ssh_url_to_repo'] + ['http_url_to_repo'] + ['web_url'] + ['readme_url'] + ['avatar_url'] + ['forks_count'] + ['star_count'] + ['last_activity_at'])    

    keywords = [keyword.strip() for keyword in keywords.split(',')]
    for keyword in keywords:
        while(ctrl):
            print(page)
            url = f"https://gitlab.com/api/v4/search?scope=projects&search={keyword}&per_page=100&page={page}"
            headers = {
                'Authorization': 'Redacted' ## 
            }
            dfs = []
            response = requests.request("GET", url, headers=headers)
            headers = response.headers
            respjson = response.json()
            for repo in respjson:
                if('description' not in repo): repo['description'] = ""
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
                

                spamwriter.writerow([repo['id']] + [desc] + [repo['name']] + [repo['name_with_namespace']] + [repo['path']] + [repo['path_with_namespace']] + [repo['created_at']] + [repo['default_branch']] + [repo['tag_list']] + [repo['ssh_url_to_repo']] + [repo['http_url_to_repo']] + [repo['web_url']] + [repo['readme_url']] + [repo['avatar_url']] + [repo['forks_count']] + [repo['star_count']] + [repo['last_activity_at']])
            if(headers['X-Next-Page']):
                page += 1
            else:
                ctrl = False