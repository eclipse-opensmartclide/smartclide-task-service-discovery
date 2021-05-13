import requests, json, re, time, concurrent.futures, random, threading
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from config import config

tokens = config.access_token_bitbucket_arr.copy()
mutex = threading.Lock()
data403 = []
data500 = []
dataErr = []

def getReposKw(kw):
    global tokens
    global data403
    global data500
    global dataErr
    print(kw)
    base_url = "https://bitbucket.org/repo/all/1?name="+kw

    # Get max pages
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, 'lxml')
    n = soup.find("section",{"class":"aui-item"})
    n2 = n.find("h1")
    rx = re.compile(r'-?\d+(?:\.\d+)?')
    numbers = rx.findall(n2.text)
    limit = (int(numbers[0])//10)+1

    data = []
    # Iterate all pages
    for i in range(1, limit+1):

        url = "https://bitbucket.org/repo/all/"+str(i)+"?name="+kw
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'lxml')
        articles = soup.find_all("article", {"class": "repo-summary"})
        print(str(i) +" / "+str(limit) + " >> "+kw + " == "+str(len(articles))+" ")
        for article in articles:
            for a in article.find_all('a', {"class": "repo-link"}, href=True):
                ctrl = True
                url = "https://api.bitbucket.org/2.0/repositories" + a['href']

                while(len(tokens)==0):
                    time.sleep(10)

                x = int(random.random()*len(tokens))
                token = tokens[x]
                headers = {
                    'access_token': token
                }
                repo = requests.get(url,headers=headers)

                # limit api requests
                while repo.status_code==429:
                    if(token in tokens):
                        tokens.remove(token)
                    print("Api limit error (429). Tokens restantes "+str(len(tokens)))
                    mutex.acquire()
                    if(len(tokens)==0):
                        time.sleep(3600)
                        tokens = config.access_token_bitbucket_arr.copy()
                        print("Tokens reiniciados")
                    mutex.release()
                    token = tokens[int(random.random()*len(tokens))]
                    headers = {
                        'access_token': token
                    }
                    repo = requests.get(url,headers=headers)

                if(repo.status_code==200):
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
                            if('next' in repo): # ToDo Revisar bucle
                                print("bucle pages")
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
                        "created_on": repo['created_on'],
                        "updated_on": repo['updated_on'],
                        "updated_on": repo['updated_on'],
                        "keyword": kw
                    }
                    data.append(datarepo)
                elif(repo.status_code==403):
                    data403.append(a['href'])
                elif(repo.status_code==500):
                    data500.append(a['href'])
                else:
                    dataErr.append(a['href']+" => "+str(repo.status_code))

        df = pd.json_normalize(data=data)
        df.to_csv(r'C:/Datos/Repos/pruebas/docker/outputsB11M/output_bitbucket_'+kw.replace(" ","")+"_" + datetime.now().strftime('%d_%m_%Y') + '.csv', index = False)

    return data

f = open("keywordsAll.txt")
keywords = []
for line in f:
    keywords.append(line.rstrip('\n'))
f.close()

data=[]
tasks = []
# Iterate keywordss
keywords_split  = np.array_split(keywords, 25)
with concurrent.futures.ThreadPoolExecutor(max_workers = len(keywords_split)) as executor:

    for kw in keywords:
        tasks.append(executor.submit(getReposKw, kw))

# iterate results
print("Tratando resultados")
for result in tasks:
    data+=result.result()

print("Generando fichero")
df = pd.json_normalize(data=data)
df.to_csv('output_bitbucket11M.csv', index = False)
print("Listo! ")

print("Generando fichero403")
df = pd.json_normalize(data=data403)
df.to_csv('output_bitbucket11M403.csv', index = False)
print("Listo! ")

print("Generando fichero500")
df = pd.json_normalize(data=data500)
df.to_csv('output_bitbucket11M500.csv', index = False)
print("Listo! ")

print("Generando ficheroErr")
df = pd.json_normalize(data=dataErr)
df.to_csv('output_bitbucket11MErr.csv', index = False)
print("Listo! ")