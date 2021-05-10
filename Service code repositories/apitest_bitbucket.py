import requests, json, re, time, concurrent.futures, random
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from config import config

tokens = config.access_token_bitbucket_arr

# Get info for repo url with api
def getRepo(urlrepo, kw):
    global dataglobal
    global tokens
    ctrl = True
    url = "https://api.bitbucket.org/2.0/repositories" + urlrepo
    token = tokens[int(random.random()*len(tokens))]
    headers = {
        'access_token': token
    }
    repo = requests.get(url,headers=headers)

    # limit api requests
    while repo.status_code==429:
        tokens.remove(token)
        print("Api limit error (429)")
        if(len(tokens)==0):
            time.sleep(3600)
            tokens = config.access_token_bitbucket_arr
        token = tokens[int(random.random()*len(tokens))]
        headers = {
            'access_token': token
        }
        repo = requests.get(url,headers=headers)

    repo = repo.json()
    while(ctrl):
        repos = ""
        #Search in the repository
        try:
            #Search README
            for f in repo['values']:
                    if('escaped_path' in f):
                        if("README" in f['escaped_path']):
                            text = requests.get(f['links']['self']['href']).text
                            lines = text.split(" ")
                            for line in lines:
                                if("github" in line):
                                    match = re.search(r'\w*github.com/\w*', line)
                                    if(match is not None):
                                        repos += match.group()
                            ctrl = False

            #Next page of files
            if('next' in repo):
                url = repo['next']
            else:
                ctrl = False
        except KeyError:
                ctrl = False
    desc = " ".join(re.split("\s+", str(repo['description']), flags=re.UNICODE))
    datarepo = {
        "full_name": repo['full_name'],
        "description": desc,
        "type": repo['type'] ,
        "link": repo['links']['clone'][0]['href'],
        "link_github": repos ,
        "language": repo['language'],
        "keyword": kw
    }
    dataglobal.append(datarepo)
    df = pd.json_normalize(data=dataglobal)
    df.to_csv(r'C:/Datos/Repos/pruebas/docker/outputsB/output_bitbucket_'+kw.replace(" ","")+"_" + datetime.now().strftime('%d_%m_%Y') + '.csv', index = False)
    return datarepo

def getReposPage(i,kw):
    # Get repos from the page
    url = "https://bitbucket.org/repo/all/"+str(i)+"?name="+kw
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    articles = soup.find_all("article", {"class": "repo-summary"})
    repos = []
    tasks = []
    for article in articles:
        for a in article.find_all('a', {"class": "avatar-link"}, href=True):

            # a['href'] => ENLACE AL REPO
            print(a['href'])
            repos.append(a['href'])

            with concurrent.futures.ThreadPoolExecutor(max_workers = 24) as executor:
                tasks.append(executor.submit(getRepo, a['href'],kw))

            # Iterate results
            for result in tasks:
                try:
                    if(result.result()!="429"):
                        data.append(result.result())


                    # Limit api requests
                    else:
                        print("Error 429, omitiendo...")
                        time.sleep(600)

                except ValueError:  # includes simplejson.decoder.JSONDecodeError
                    print("ValueError")

    df = pd.json_normalize(data=data)
    df.to_csv('output_bitbucket3.csv', index = False)

def getReposKw(kw):
    print(kw)
    cont = 1 # Page counter
    base_url = "https://bitbucket.org/repo/all/"+str(cont)+"?name="+kw
    tasks = []

    # Get max pages
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, 'lxml')
    n = soup.find("section",{"class":"aui-item"})
    n2 = n.find("h1")
    rx = re.compile(r'-?\d+(?:\.\d+)?')
    numbers = rx.findall(n2.text)
    limit = (int(numbers[0])//10)+1

    # Iterate all pages
    for i in range(1, limit):
        print(str(i) +" / "+str(limit))
        with concurrent.futures.ThreadPoolExecutor(max_workers = 24) as executor:
            tasks.append(executor.submit(getReposPage, i,kw))

f = open("keywords.txt")
keywords = []
for line in f:
    keywords.append(line.rstrip('\n'))
f.close()

dataglobal = []
data=[]
tasks = []
keywords_split  = np.array_split(keywords, 24) # max workers
for split in range(len(keywords_split)):
    with concurrent.futures.ThreadPoolExecutor(max_workers = len(keywords_split)) as executor:
        for kw in keywords_split[split]:
            tasks.append(executor.submit(getReposKw, kw))