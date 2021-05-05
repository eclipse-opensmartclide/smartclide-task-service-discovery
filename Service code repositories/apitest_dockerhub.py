# -*- coding: utf-8-sig -*-
import subprocess, requests, re, concurrent.futures
import pandas as pd
import numpy as np

base_url2 = "https://github.com/"
base_url = "https://hub.docker.com/r/"
limit = 100

def search_dockerhub_files(keyword):
    print(keyword)
    process = subprocess.Popen(['docker', 'search', '--limit', str(limit),'--no-trunc', keyword],
                            stdout=subprocess.PIPE,
                            universal_newlines=True) # stdout.decode('utf-8')

    data = []
    for i in range(1,limit):
        print(str(i)+" / "+keyword)
        try:
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
                nums = rx.findall(output2)
                if(len(nums)!=0):
                    line = output2.split(nums[len(nums)-1])
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
                        for i in range(0,len(desc)):
                            descr +=desc[i]
                    else:
                        descr = desc[0]
                    desc.append("")
                    url = f"{base_url}{name}"
                    url2 = f"{base_url2}{name}"
                    if(url!="{ base_url }"):
                        r = requests.get(url2)
                        if(r.status_code==404 or r.status_code==403):
                            url2 = "not found"
                        datarepo = {
                            "name": name,
                            "description": descr,
                            "stars": numbers[len(numbers)-1],
                            "official": off,
                            "automated": aut,
                            "github": url2,
                            "keyword": keyword
                        }
                        data.append(datarepo)
        except UnicodeDecodeError:
            print("error")
    return data

df_temp = []

def process_(bulk, num_Splits):
    global df_temp
    # Split
    bulk_splited  = np.array_split(bulk, num_Splits) # max workers

    tasks = []

    for split in range(len(bulk_splited)):
        with concurrent.futures.ThreadPoolExecutor(max_workers = len(bulk_splited)) as executor:
            for data in bulk_splited[split]:
                tasks.append(executor.submit(search_dockerhub_files, data))

    # iterate results
    for result in tasks:
        if(type(result.result() is not list)):
            if(result.result() is not None):
                df_temp += result.result()
    df = pd.json_normalize(data=df_temp)
    df.to_csv('output_dockerhub.csv', index = False, encoding='utf-8-sig')

    return df_temp


f = open("keywordsAll.txt")
keywords = []
for line in f:
    keywords.append(line.rstrip('\n'))
f.close()

df_final = pd.DataFrame()
df_final = process_(keywords, 16)
df = pd.json_normalize(data=df_final)
df.to_csv('output_dockerhub.csv', index = False,encoding='utf-8-sig')