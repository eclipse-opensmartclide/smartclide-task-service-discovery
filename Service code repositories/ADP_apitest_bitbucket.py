import subprocess, requests, json, re, csv, threading, requests
from bs4 import BeautifulSoup

def getRepo(urlrepo):
    ctrl = True
    url = "https://api.bitbucket.org/2.0/repositories" + urlrepo
    repo = requests.get(url)
    repo = repo.json()
    while(ctrl):
        repos = ""
        #Search in the repository
        resp2 = requests.get(url)
        respjson = resp2.json()
        try: 
            #Search README
            for f in respjson['values']:
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
            if('next' in respjson):
                url = respjson['next']
            else:
                ctrl = False     
        except KeyError: 
                ctrl = False     
    desc = " ".join(re.split("\s+", repo['description'], flags=re.UNICODE))
    spamwriter.writerow([repo['full_name']] + [desc] + [repo['type']] + [repo['links']['clone'][0]['href']] + [repos] + [repo['language']])

#Get repositories
keywords = 'service, api'
cont = 1
base_url = "https://bitbucket.org/repo/all/"+str(cont)+"?name="
with open('reposlistbitbucket.csv', 'w', newline='',encoding="utf-8") as csvfile:
    spamwriter = csv.writer(csvfile)
    spamwriter.writerow(['Name of Sample Source Code'] + ['Description'] + ['Category'] + ['Source Code'] + ['Repository'] + ['Languages'])
    keywords = [keyword.strip() for keyword in keywords.split(',')]
    for keyword in keywords:
        r = requests.get(base_url+"services")
        soup = BeautifulSoup(r.text, 'lxml')
        #Get total pages
        n = soup.find("section",{"class":"aui-item"})
        n2 = n.find("h1")
        rx = re.compile(r'-?\d+(?:\.\d+)?')
        numbers = rx.findall(n2.text)
        while(cont<=int(numbers[0])): 
            print(cont)
            base_url = "https://bitbucket.org/repo/all/"+str(cont)+"?name="
            r = requests.get(base_url+"services")
            soup = BeautifulSoup(r.text, 'lxml')
            articles = soup.find_all("article", {"class": "repo-summary"})
            for article in articles:
                for a in article.find_all('a', {"class": "avatar-link"}, href=True):
                    hilo = threading.Thread(target=getRepo,args=(a['href'], ))    
                    hilo.start()
            cont +=1
            

