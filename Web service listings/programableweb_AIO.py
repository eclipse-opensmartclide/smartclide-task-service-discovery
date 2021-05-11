# -*- coding: utf-8 -*-
"""Programableweb.ipynb



!pip install requests-random-user-agent
!pip install pandas
!pip install bs4
!pip install lxml

"""

import time
from datetime import datetime
import pandas as pd
import numpy as np
import concurrent.futures
import requests
import requests_random_user_agent
from bs4 import BeautifulSoup

def _download_programmableweb_list(urlsSplited):
    """
    Iterates over the input list and accesses the url 
     to collect data using requests and BeautifulSoup
    
    :param urlsSplited: List
    :return DataFrame:
    """
    df_temp = pd.DataFrame()

    for url in urlsSplited:

        df_url = pd.DataFrame()

        rq = requests.get(url)

        if rq.status_code != 200:
            continue

        main_data = rq.text
        # dataset from table
        df_url = pd.read_html(main_data, index_col=0)[0]

        main_soup = BeautifulSoup(main_data, 'html.parser')
        main_names = main_soup.find_all('tr')[1:245]

        list_url = []
        # Head url for meta_url
        meta_Url = 'https://www.programmableweb.com'

        for row in main_names:
            text = row.find_all('td')[0]
            list_url.append((meta_Url + str(text).partition('<a href="')
                            [2].partition('">')[0]))  # URL to specific page

        df_url['Meta_Url'] = list_url

        df_temp = df_temp.append(df_url, ignore_index=True)

    return df_temp

def download_programmableweb_list(bulkUrls, numSplits):
    """
    Receives the url list, bulkUrls, and a number of partitions, numSplits, to launch concurrent 
     executions by calling _download_programmableweb_list which fetches
     the data for each partition from the list and returns a DataFrame
     
    :param bulkUrls: List
    :param numSplits: int
    :return DataFrame:
    """
    df_temp = pd.DataFrame()
    # Split urls
    urls_splited = np.array_split(bulkUrls, numSplits)  # max workers

    # Remove empty lists from the split
    #urls_splited = [i for i in urls_splited if i]
    #urls_splited = list(filter(None, urls_splited))

    tasks = []

    for split in range(len(urls_splited)):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls_splited)) as executor:
            tasks.append(executor.submit(
                _download_programmableweb_list, urls_splited[split]))
    # Union
    for result in tasks:
        df_temp = df_temp.append(result.result())

    return df_temp

def download_list(headUrl, numPages, numSplits):
    """
    Builds a list of urls based on the headUrl, then calls 
     download_programmableweb_list which gets the entries of each url
     to finally return a basic dataframe 
     
    :param headUrl: String
    :param df_temp: DataFrame
    :param numSplits: int
    :return DataFrame:
    """
    urls = []
    df_temp = pd.DataFrame()

    for i in range(numPages):
        main_url = headUrl + str(i)
        urls.append(main_url)

    df_temp = download_programmableweb_list(urls, numSplits)
    return df_temp

def _download_meta_url(df_temp, listType):
    """
    Iterates over the input dataframe and accesses the metaurl 
     to collect data based in listType imput using requests and BeautifulSoup
    
    :param df_temp: DataFrame
    :param listType: String
    :return DataFrame:
    """

    df_temp = df_temp.reset_index(drop=True)

    for i in range(len(df_temp)):

        meta_url = df_temp['Meta_Url'][i]

        rq = requests.get(meta_url)

        while rq.status_code == 429: # Too many rq
          time.sleep(2100)
          rq = requests.get(meta_url)
          if rq.status_code == 200:
            break
          # Sleep, proxy change?          

        meta_data = rq.text
        meta_soup = BeautifulSoup(meta_data, 'html.parser')

        # Update Description from the meta url, api-library LIB do not have description
        if listType == "LIB":
            None
        else:
            if listType == "MASH":
                # TODO: simplificar
                meta_description = str(meta_soup.find('div', class_='tabs-header_description')).partition(
                    '">')[2].partition('</')[0].partition('">')[2].partition('">')[2].partition('">')[2]
                df_temp['Description'][i] = meta_description
            else: # CODE, SDK, FRAME             
                meta_description = str(meta_soup.find('div', class_='tabs-header_description')).partition('">')[2].partition('</')[0]       
                df_temp['Description'][i] = meta_description

        # Get section specs and iterate the labels
        meta_specs = meta_soup.find('div', class_='section specs')
        for lab in meta_specs.select("label"):

            if listType == "MASH":
                # Search for Related APIs
                if (lab.text.lower().find("related apis") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Related APIs'][i] = lab.find_next_sibling().text
                    continue

                # Search for Categories
                if (lab.text.lower().find("categories") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Categories'][i] = lab.find_next_sibling().text
                    continue

                # Search for URL
                if (lab.text.lower().find("url") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['URL'][i] = lab.find_next_sibling().text
                    continue

                # Search for Company
                if (lab.text.lower().find("company") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Company'][i] = lab.find_next_sibling().text
                    continue

                # Search for App Type
                if (lab.text.lower().find("app type") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['App Type'][i] = lab.find_next_sibling().text
                    continue
                continue

            if listType == "SDK":
                # Search for Related APIs
                if (lab.text.lower().find("related apis") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Related APIs'][i] = lab.find_next_sibling().text
                    continue

                # Search for Languages
                if (lab.text.lower().find("languages") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Languages'][i] = lab.find_next_sibling().text
                    continue

                # Search for Categories and remplace them
                if (lab.text.lower().find("categories") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Category'][i] = lab.find_next_sibling().text
                    continue

                # Search for Provider
                if (lab.text.lower().find("sdk provider") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Provider'][i] = lab.find_next_sibling().text
                    continue

                # Search for Provider
                if (lab.text.lower().find("url") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Asset URL'][i] = lab.find_next_sibling().text
                    continue

                # Search for Provider
                if (lab.text.lower().find("repository") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Repository'][i] = lab.find_next_sibling().text
                    continue
                continue

            if listType == "FRAME":
                # Search for Languages
                if (lab.text.lower().find("languages") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Languages'][i] = lab.find_next_sibling().text
                    continue

                # Search for Provider
                if (lab.text.lower().find("provider") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Provider'][i] = lab.find_next_sibling().text
                    continue

                # Search for Asset URL
                if (lab.text.lower().find("asset home") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Asset URL'][i] = lab.find_next_sibling().text
                    continue

                # Search for Repository
                if (lab.text.lower().find("repository") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Repository'][i] = lab.find_next_sibling().text
                    continue

                # Search for TOS
                if (lab.text.lower().find("terms of service") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Terms Of Service'][i] = lab.find_next_sibling().text
                    continue
                continue

            if listType == "CODE":
                # Search for Repo
                if (lab.text.lower().find("repository") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Repository'][i] = lab.find_next_sibling().text
                    continue

                # Search for Source Code
                if (lab.text.lower().find("link to source code") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Source Code'][i] = lab.find_next_sibling().text
                    continue

                # Search for Categories and remplace them
                if (lab.text.lower().find("categories") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Category'][i] = lab.find_next_sibling().text
                    continue

                # Search for Languages
                if (lab.text.lower().find("languages") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Languages'][i] = lab.find_next_sibling().text
                    continue
                continue

            if listType == "LIB":
                # Search for Related APIs
                if (lab.text.lower().find("related apis") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Related APIs'][i] = lab.find_next_sibling().text
                    continue
                # Search for Languages
                if (lab.text.lower().find("languages") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Languages'][i] = lab.find_next_sibling().text
                    continue
                # Search for Framework
                if (lab.text.lower().find("related framework") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Related Frameworks'][i] = lab.find_next_sibling().text
                    continue
                # Search for Categories and remplace them
                if (lab.text.lower().find("categories") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Category'][i] = lab.find_next_sibling().text
                    continue
                # Search for Architectural
                if (lab.text.lower().find("architectural style") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Architectural Style'][i] = lab.find_next_sibling().text
                    continue
                # Search for Provider
                if (lab.text.lower().find("library provider") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Provider'][i] = lab.find_next_sibling().text
                    continue
                # Search for URL
                if (lab.text.lower().find("asset home") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Asset URL'][i] = lab.find_next_sibling().text
                    continue
                # Search for Provider
                if (lab.text.lower().find("repository") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Repository'][i] = lab.find_next_sibling().text
                    continue
                # Search for Provider
                if (lab.text.lower().find("terms of service") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Terms Of Service'][i] = lab.find_next_sibling().text
                    continue
                # Search for Provider
                if (lab.text.lower().find("type") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Type'][i] = lab.find_next_sibling().text
                    continue

                # Search for Provider
                if (lab.text.lower().find("docs home") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Docs Home'][i] = lab.find_next_sibling().text
                    continue

                # Search for Provider
                if (lab.text.lower().find("request formats") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Request Formats'][i] = lab.find_next_sibling().text
                    continue

                # Search for Provider
                if (lab.text.lower().find("response formats") > -1):
                    #print(lab.text + ": " + lab.find_next_sibling().text)
                    df_temp['Response Formats'][i] = lab.find_next_sibling().text
                    continue
                continue

    return df_temp

def download_programmableweb_meta_url(df, numSplits, listType):
    """
    Splits a dataframe and uses simultaneous executions to access the metaurl
    wirh _download_meta_url the information is collected and returned
    
    :param df: DataFrame
    :param numSplits: int
    :param numSplits: string
    :return DataFrame:
    """
    df_temp = pd.DataFrame()
    tasks = []

    # Split dataframe
    dt_splited = np.array_split(df, numSplits)  # max workers

    # Remove empty dataframes from the split
    dt_splited = list(filter(lambda df: not df.empty, dt_splited))

    for split in range(len(dt_splited)):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(dt_splited)) as executor:
            # Download splited dataframes with jobs
            tasks.append(executor.submit(_download_meta_url, dt_splited[split], listType))
    # Union
    for result in tasks:
        print("Task finisehd", result)
        df_temp = df_temp.append(result.result())

    return df_temp

def download_MASH():
    """
    Creates a DataFrame from mashup programmableweb URL

    :return DataFrame:
    """
    df_temp = pd.DataFrame()
    df_temp = download_list(MASH_URL, MASH_PAGES, 5)
    df_temp = df_temp.reset_index(drop=True)

    # Creates new columns for the data in the meta url
    df_temp['Related APIs'] = ""
    df_temp['Categories'] = ""
    df_temp['URL'] = ""
    df_temp['Company'] = ""
    df_temp['App Type'] = ""

    df_temp = download_programmableweb_meta_url(df_temp, 5, "MASH")
    df_temp = df_temp.reset_index(drop=True)
    return df_temp

def download_FRAME():
    """
    Creates a DataFrame from framework programmableweb URL

    :return DataFrame:
    """
    df_temp = pd.DataFrame()
    df_temp = download_list(FRAME_URL, FRAME_PAGES, 5)
    df_temp = df_temp.reset_index(drop=True)

    # Creates new columns
    df_temp['Languages'] = ""
    df_temp['Provider'] = ""
    df_temp['Asset URL'] = ""
    df_temp['Repository'] = ""
    df_temp['Terms Of Service'] = ""

    df_temp = download_programmableweb_meta_url(df_temp, 5, "FRAME")
    df_temp = df_temp.reset_index(drop=True)
    return df_temp

def download_CODE():
    """
    Creates a DataFrame from api-library programmableweb URL

    :return DataFrame:
    """
    df_temp = pd.DataFrame()
    df_temp = download_list(CODE_URL, CODE_PAGES, 5)
    df_temp = df_temp.reset_index(drop=True)

    # Creates new columns
    df_temp['Source Code'] = ""
    df_temp['Repository'] = ""
    df_temp['Languages'] = ""

    df_temp = download_programmableweb_meta_url(df_temp, 5, "CODE")
    df_temp = df_temp.reset_index(drop=True)
    return df_temp

def download_SDK():
    """
    Creates a DataFrame from sample-source-code programmableweb URL

    :return DataFrame:
    """
    df_temp = pd.DataFrame()
    df_temp = download_list(SDK_URL, SDK_PAGES, 5)    
    df_temp = df_temp.reset_index(drop=True)

    # Creates new columns
    df_temp['Description'] = ""
    df_temp['Repository'] = ""
    df_temp['Languages'] = ""
    df_temp['Provider'] = ""
    df_temp['Asset URL'] = ""

    print("Downloading Meta URL data")
    df_temp = download_programmableweb_meta_url(df_temp, 5, "SDK")
    df_temp = df_temp.reset_index(drop=True)
    return df_temp

def download_LIB():
    """
    Creates a DataFrame from api-library programmableweb URL

    :return DataFrame:
    """
    df_temp = pd.DataFrame()
    df_temp = download_list(LIB_URL, LIB_PAGES, 5)
    df_temp = df_temp.reset_index(drop=True)

    # Creates new columns
    df_temp['Languages'] = ""
    df_temp['Related Frameworks'] = ""
    df_temp['Category'] = ""
    df_temp['Architectural Style'] = ""
    df_temp['Provider'] = ""
    df_temp['Asset URL'] = ""
    df_temp['Repository'] = ""
    df_temp['Terms Of Service'] = ""
    df_temp['Type'] = ""
    df_temp['Docs Home'] = ""
    df_temp['Request Formats'] = ""
    df_temp['Response Formats'] = ""

    df_temp = download_programmableweb_meta_url(df_temp, 5, "LIB")
    df_temp = df_temp.reset_index(drop=True)
    return df_temp
    
"""### URL and PAGES
    TODO: Get num of pages requesting the Url

"""
LIB_URL = "https://www.programmableweb.com/category/all/api-library?page="
LIB_PAGES = 67 # 67

MASH_URL = "https://www.programmableweb.com/category/all/mashups?page="
MASH_PAGES = 1 # 258

FRAME_URL = "https://www.programmableweb.com/category/all/web-development-frameworks?page="
FRAME_PAGES = 23 # 23

CODE_URL = "https://www.programmableweb.com/category/all/sample-source-code?page="
CODE_PAGES = 616 #616

SDK_URL = "https://www.programmableweb.com/category/all/sdk&page="
SDK_PAGES = 50 #776

"""### Wait btw types to avoid 429

TODO?: handle 429 with proxys?

"""

print("SDK.. ")
df_sdk = download_SDK()
df_sdk

df_sdk.to_csv(r'/content/sample_data/SDK_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index = True, header = True)

# time.sleep()

print("CODE.. ")
df_code = download_CODE()
df_code

df_code.to_csv(r'/content/sample_data/CODE_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index = True, header = True)

# time.sleep()

print("FRAME.. ")
df_frame = download_FRAME()
df_frame

df_frame.to_csv(r'/content/sample_data/FRAME_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index = True, header = True)

# time.sleep()

print("LIB.. ")
df_lib = download_LIB()
df_lib

df_lib.to_csv(r'/content/sample_data/LIB_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index = True, header = True)

# time.sleep()

print("MASH.. ")
df_mash = download_MASH()
df_mash

df_mash.to_csv(r'/content/sample_data/MASH_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index = True, header = True)

# time.sleep()




