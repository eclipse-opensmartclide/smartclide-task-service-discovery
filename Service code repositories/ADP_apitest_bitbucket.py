import requests, json, re, time, concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
from config import config

# Get info for repo url with api
def getRepo(urlrepo, kw):
    ctrl = True
    url = "https://api.bitbucket.org/2.0/repositories" + urlrepo
    headers = { "access_token": config.access_token_bitbucket}
    repo = requests.get(url,headers=headers)
    # limit api requests
    while repo.status_code==429:
        print("Api limit error (429)")
        time.sleep(1800)


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
    return datarepo

f = open("keywordsBit.txt")
keywords = []
for line in f:
    keywords.append(line.rstrip('\n'))
f.close()

data=[]
for kw in keywords:
    print(kw)
    cont = 1 # Page counter
    base_url = "https://bitbucket.org/repo/all/"+str(cont)+"?name="+kw

    # Get max pages
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, 'lxml')
    n = soup.find("section",{"class":"aui-item"})
    n2 = n.find("h1")
    rx = re.compile(r'-?\d+(?:\.\d+)?')
    numbers = rx.findall(n2.text)
    limit = (int(numbers[0])//10)+1
    print(limit)
    # Iterate all pages
    for i in range(1,limit):
        print(str(i) +" / "+str(limit))

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

                with concurrent.futures.ThreadPoolExecutor(max_workers = 4) as executor:
                    tasks.append(executor.submit(getRepo, a['href'],kw))

        # Iterate results
        for result in tasks:
            try:
                if(result.result()!="429"):
                    data.append(result.result())

                # Limit api requests
                else:
                    print("Error 429, omitiendo...")
                    time.sleep(3600)

            except ValueError:  # includes simplejson.decoder.JSONDecodeError
                print("ValueError")

        df = pd.json_normalize(data=data)
        df.to_csv('output_bitbucket.csv', index = False)
