# -*- coding: utf-8 -*-
"""SourceCode_pandas web scraping.ipynb

# Sample Source Code

## Pandas HTML + BeautifulSoup

+600 web requests


!pip install requests-random-user-agent
!pip install pandas
!pip install bs4
!pip install lxml

"""
import multiprocessing
multiprocessing.cpu_count()

import requests
import requests_random_user_agent
#s = requests.Session()
#print(s.headers['User-Agent'])

# Without a session
resp = requests.get('https://httpbin.org/user-agent')
print(resp.json()['user-agent'])

import concurrent.futures
import requests
import requests_random_user_agent
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

def _download_sourceCode(url):

  rq = requests.get(url)
  
  if rq.status_code == 404 or rq.status_code == 403: ## Handle more error codes...
    exit

  main_data = rq.text

  # dataset from table
  df_tempSourceCode = pd.read_html(main_data, index_col=0)[0]

  main_soup = BeautifulSoup(main_data, 'html.parser')
  main_names = main_soup.find_all('tr')[1:245]

  list_urlSourceCode = []

  # Head url for meta_url
  head_Url= 'https://www.programmableweb.com'

  for row in main_names:
      text = row.find_all('td')[0]
      list_urlSourceCode.append( (head_Url + str(text).partition('<a href="')[2].partition('">')[0]))

  df_tempSourceCode['Meta_Url'] = list_urlSourceCode

  return df_tempSourceCode


def download_sourceCode(bulk_urls, num_Splits):
  df_temp = pd.DataFrame()

  # Split urls
  urls_splited  = np.array_split(bulk_urls, num_Splits) # max workers

  tasks = []

  for split in range(len(urls_splited)):
    with concurrent.futures.ThreadPoolExecutor(max_workers = len(urls_splited)) as executor:
      for url in urls_splited[split]:    
        tasks.append(executor.submit(_download_sourceCode, url))
       ## mb call with bulk urls instead of one by one
  # Union
  for result in tasks:
    df_temp = df_temp.append(result.result())

  return df_temp

sourceCode_urls=[]
for i in range(616): ## web pages?.. 615+1
    main_url = 'https://www.programmableweb.com/category/all/sample-source-code?page=' + str(i) ## parametrizar + comprobar que hay datos
    sourceCode_urls.append(main_url)

df_sourceCode = pd.DataFrame()
df_sourceCode = download_sourceCode(sourceCode_urls, 10)
df_sourceCode

"""## Meta URL Processing

+15k web requests



"""

# Creates new columns
df_sourceCode['Source Code'] = ""
df_sourceCode['Repository'] = ""
df_sourceCode['Languages'] = ""

def _download_meta_url(df_source):

  for i in range(len(df_source)):

    #print(df_source)
    meta_url = df_source['Meta_Url'][i]
    rq = requests.get(meta_url)

    if rq.status_code == 404 or rq.status_code == 403: ## Handle more error codes...
      continue

    meta_data = rq.text
    meta_soup = BeautifulSoup(meta_data, 'html.parser')

    # Update Description from the meta url
    meta_description = str(meta_soup.find('div', class_='tabs-header_description')).partition('">')[2].partition('</')[0]
    df_source['Description'][i] = meta_description 

    meta_specs = meta_soup.find('div', class_='section specs')

    for lab in meta_specs.select("label"):   

      # Search for Repo
      if (lab.text.lower().find("repository") > -1):
          #print(lab.text + ": " + lab.find_next_sibling().text)
          df_source['Repository'][i] =   lab.find_next_sibling().text

       # Search for Source Code
      if (lab.text.lower().find("link to source code") > -1):
          #print(lab.text + ": " + lab.find_next_sibling().text)
          df_source['Source Code'][i] =   lab.find_next_sibling().text

      # Search for Categories and remplace them
      if (lab.text.lower().find("categories") > -1):
         #print(lab.text + ": " + lab.find_next_sibling().text)
          df_source['Category'][i] =   lab.find_next_sibling().text

      # Search for Languages
      if (lab.text.lower().find("languages") > -1):
          #print(lab.text + ": " + lab.find_next_sibling().text)
          df_source['Languages'][i] =   lab.find_next_sibling().text
    #print(df_source)

  return df_source


def download_meta_url(df_sourceCode, num_Splits):
  # Split dataframe
  dtframe_splited  = np.array_split(df_sourceCode, num_Splits) # max workers
  tasks = []

  for split in range(len(dtframe_splited)):
    with concurrent.futures.ThreadPoolExecutor(max_workers = len(dtframe_splited)) as executor:
      # Download dataframes splited
      tasks.append(executor.submit(_download_meta_url, dtframe_splited[split]))

  df_temp = pd.DataFrame()
  # Union
  for result in tasks:
    df_temp = df_temp.append(result.result())

  return df_temp

df_sourceCode_metaData = pd.DataFrame()
df_sourceCode_metaData = download_meta_url(df_sourceCode, 10)
df_sourceCode_metaData.head()

# Meta_Url could be used to check for updates on the source website. That uses only +600 web requests instead of +15k
# save a copy of the original dataframe to check for updates based on the meta url or other fields
df_sourceCode_metaData.reset_index(inplace=True)
df_export_sourceCode = df_sourceCode_metaData.copy()

# Drop the column for the data analysis ( ? ? )
df_sourceCode_metaData.drop('Meta_Url', inplace=True, axis=1)
df_sourceCode_metaData

"""## Export"""

from datetime import datetime
datetime = datetime.now()

## Export the data for analysis 
# To CSV (index True 0,1,2...)
df_export_sourceCode.to_csv(r'/content/DataFrame/source_code_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index = True, header = True)

# To JSON (columns format index True 0,1,2...)
df_export_sourceCode.to_json(r'/content/DataFrame/source_code_' + datetime.now().strftime('%d_%m_%Y') + '.json')


## Export the original + Meta_Url

# To CSV (index True 0,1,2...)
df_sourceCode.to_csv(r'/content/DataFrame/original_source_code_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index = True, header = True)

# To JSON (columns format index True 0,1,2...)
df_sourceCode.to_json(r'/content/DataFrame/original_source_code_' + datetime.now().strftime('%d_%m_%Y') + '.json')