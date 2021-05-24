import requests, json, re, time, concurrent.futures, threading, requests_random_user_agent
import pandas as pd
import numpy as np
from config import config
from bs4 import BeautifulSoup
from datetime import datetime

# Initial vars
data=[]
tasks = []
dataErr = []
keywords = []
del_columns=[
    'connectActions',
    'cloneProtocol',
    'mirrors',
    'menuItems',
    'bitbucketActions',
    'activeMenuItem',
    'currentRepository.links.clone',
    'currentRepository.links.self.href',
    'currentRepository.links.avatar.href',
    'currentRepository.project.links.self.href',
    'currentRepository.project.links.avatar.href',
    'currentRepository.project.owner.links.self.href',
    'currentRepository.project.owner.links.avatar.href',
    'currentRepository.project.workspace.links.self.href',
    'currentRepository.project.workspace.links.avatar.href',
    'currentRepository.project.is_private',
    'currentRepository.owner.links.self.href',
    'currentRepository.owner.links.avatar.href',
    'currentRepository.owner.is_active',
    'currentRepository.owner.department',
    'currentRepository.owner.has_2fa_enabled',
    'currentRepository.is_private',
    'currentRepository.project.owner.uuid',
    'currentRepository.project.workspace.uuid',
]
mutex = threading.Lock()

def getRequest(url):
    """
    Create request to param url

    :param url: str
    :return ResulSet:
    """
    headers = {
        'access_token': config.access_token_bitbucket_arr_2
    }
    r = requests.get(url,headers=headers)
    while(r.status_code==429): # If api limit, sleep 120s
        print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tError 429 "+url)
        time.sleep(120)
        r = requests.get(url,headers=headers)
    return r

def getInfoReposFromKw(kw):
    """
    Create json list with all information from bitbucket search with keyword

    :param kw: str
    :return List:
    """
    global dataErr

    # Start keyword & Get max pages
    print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \t"+kw +" >> \tEMPEZANDO ")
    r = getRequest("https://bitbucket.org/repo/all/1?name="+kw)
    soup = BeautifulSoup(r.text, 'lxml')
    n = soup.find("section",{"class":"aui-item"})
    n2 = n.find("h1")
    rx = re.compile(r'-?\d+(?:\.\d+)?')
    numbers = rx.findall(n2.text)
    limit = (int(numbers[0])//10)+1

    # Iterate all pages
    data = []
    for i in range(1, limit+1):

        # Take information from i page
        r = getRequest("https://bitbucket.org/repo/all/"+str(i)+"?name="+kw)
        soup = BeautifulSoup(r.text, 'lxml')
        articles = soup.find_all("article", {"class": "repo-summary"})
        print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \t"+kw + " >> \t"+str(i) +" / "+str(limit) + " == "+str(len(articles))+" ")

        # Iterate all repos in page
        for article in articles:
            for a in article.find_all('a', {"class": "repo-link"}, href=True):
                repo = getRequest("https://bitbucket.org" + a['href'])

                # Append information repo to json list
                if(repo.status_code==200):
                    repotext = repo.text.split('window.__initial_state__ = {"section": {"repository": ')
                    if(len(repotext)>1):
                        datajson = repotext[1].split('}, "global":')[0]
                        datajson = " ".join(re.split(r"\s+", datajson, flags=re.UNICODE))
                        datajson = " ".join(re.split(r"\\n+", datajson, flags=re.UNICODE))
                        datajson = " ".join(re.split(r"\\t+", datajson, flags=re.UNICODE))
                        datajson = " ".join(re.split(r"\\r+", datajson, flags=re.UNICODE))
                        datajson = " ".join(re.split(r"\\r\\n+", datajson, flags=re.UNICODE))
                        data.append(json.loads(datajson))
                    else:
                        dataErr.append({
                            "link": a['href'],
                            "error": "No info",
                            "keyword": kw
                        })
                else:
                    dataErr.append({
                        "link": a['href'],
                        "error": str(repo.status_code),
                        "keyword": kw
                    })

        # Create security copy csv
        generate_file('bitbucket_19M/bitbucket_'+kw.replace(" ","")+"_" + datetime.now().strftime('%d_%m_%Y') + '.csv',data,True)

    # Return json list
    return data

def generate_file(name,data,delete):
    """
    Generate file with that configuration

    :param name: str
    :param data: List
    :param delete: bool
    :return List:
    """
    # Create dataframe from json list
    print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tGenerando fichero"+name)
    df = pd.json_normalize(data=data)
    if(len(df.columns)>0 and delete):
        # Delete some columns
        df = df.drop(columns=del_columns,axis=1)
    # Generate file
    df.to_csv('outputs/'+name, index = False,encoding='utf-8-sig')
    print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tListo! ")

# Take all keywords from file
f = open("keywords.txt")
for line in f:
    keywords.append(line.rstrip('\n'))
f.close()

# Iterate keywords on thread
keywords_split  = np.array_split(keywords, 10)
with concurrent.futures.ThreadPoolExecutor(max_workers = len(keywords_split)) as executor:
    for kw in keywords:
        tasks.append(executor.submit(getInfoReposFromKw, kw))

# Iterate results
print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tTratando resultados")
for result in tasks:
    data+=result.result()

# Generate data file & error file
generate_file('bitbucket2_'+ datetime.now().strftime('%d_%m') + '.csv',data,True)
if(dataErr):
    generate_file('bitbucket2_'+ datetime.now().strftime('%d_%m') + '_err.csv',dataErr,False)