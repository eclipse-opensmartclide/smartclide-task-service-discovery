import subprocess
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re

# More filters: 
# https://docs.docker.com/engine/reference/commandline/search/#filtering

#   Examples
#    docker search ruby
#    docker search wsdl
#    docker search weather-service
#    docker search service

base_url2 = "https://github.com/"
base_url = "https://hub.docker.com/r/"
limit = 25

def search_dockerhub_files(keywords):
    keywords = [keyword.strip() for keyword in keywords.split(',')]

    for keyword in keywords:
        process = subprocess.Popen(['docker', 'search', '--limit', str(limit),'--no-trunc', keyword], 
                                stdout=subprocess.PIPE,
                                universal_newlines=True) # stdout.decode('utf-8')

        d = []
        for i in range(0,limit+1):
            output2 = process.stdout.readline()

            output = " ".join(re.split("\s+", output2, flags=re.UNICODE))

            linea=output.split(" ")
            linea.append('')
            linea.append('')
            linea.append('')
            name = linea[0]
            linea[0] = ""
            
            if(name!="NAME"):
                
                rx = re.compile(r'-?\d+(?:\.\d+)?')
                numbers = rx.findall(output2)
                line = output2.split(numbers[len(numbers)-1])
                if(len(line[1])==18):
                    off = "1"
                    aut = "0"
                elif(len(line[1])==19):
                    off = "1"
                    aut = "0"
                elif(len(line[1])==25):
                    aut = "1"
                    off = "0"
                else:
                    off = "0"
                    aut = "0"
                
                linea = " ".join(linea)
                rx = re.compile(r'-?\d+(?:\.\d+)?')
                numbers = rx.findall(linea)
                
                desc = linea.split(numbers[len(numbers)-1])
                if(len(numbers)>1):
                    descr = ""
                    for i in range(0,len(numbers)):
                        descr +=desc[i]
                else:
                    descr = desc[0]
                desc.append("")
                url = f"{base_url}{name}"
                url2 = f"{base_url2}{name}"
                if(url!="{ base_url }"):

                    r = requests.get(url2)
                    soup = BeautifulSoup(r.text, 'lxml')
                    title = soup.find('title')
                    if(title.text != "Page not found · GitHub · GitHub"):
                        d.append([name,descr,numbers[len(numbers)-1],off,aut,url2])
                    else:
                        d.append([name,descr,numbers[len(numbers)-1],off,aut,"not found"])
                else:
                    break
            else:
                d.append([name,linea[1],linea[2],linea[3],linea[4],"GITHUB"])

        df = pd.DataFrame(np.array(d))
        print(df)

searchs = 'service, api'
search_dockerhub_files(searchs)