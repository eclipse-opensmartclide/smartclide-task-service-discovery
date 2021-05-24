import pandas as pd
import time
import calendar
import requests
from datetime import datetime
import numpy as np
import concurrent.futures

from github import Github
from github import RateLimitExceededException
from github import GithubException

ACCESS_TOKEN = ''  # Github persoal API
g = Github(ACCESS_TOKEN)

# We look at the readme and the project description

def search_github_repos(keywords):
    global g

    # Split keywords in case of multiple
    keywords = [keyword.strip() for keyword in keywords.split(',')]

    # The query is set
    query = '+'.join(keywords) + '+in:readme+in:description'

    result = g.search_repositories(query, 'stars', 'desc')

    # Limited to 1k for search
    print(f'Found {result.totalCount} repo(s) with the keywords',
          ','.join(keywords))
          
    df_temp = pd.DataFrame()
    df_temp['Url'] = ""
    df_temp['Description'] = ""
    df_temp['Topics'] = ""
    df_temp['Stars'] = ""

    iter_obj = iter(result)
    while True:      
        try: 
            for repo in result:
            
                time.sleep(0.1)                
                cloneUrl = str({repo.clone_url}).replace(
                    "{'", "").replace("'}", "")
                description = str({repo.description}).replace(
                    "{'", "").replace("'}", "")
                stars = str({repo.stargazers_count}).replace(
                    "{", "").replace("}", "")
                topics = ','.join(repo.get_topics())

                df_repo = pd.DataFrame(
                    {'Url': cloneUrl, 'Description': description, 'Topics': topics, 'Stars': stars}, index=[0])

                df_temp = df_temp.append(df_repo)

                # Dw readme?
                #READMEcontents = repo.get_contents("readme.md")

            df_temp = df_temp
            # One file per keyword
            df_temp.to_csv(r'./Github/' + 'GitHub_' + ','.join(keywords) +
                           '_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index=True, header=True)
            
            raise StopIteration

        except StopIteration:
            time.sleep(1)
            break
        
        except GithubException:
            print("\nGithubException 503 ?")            
            time.sleep(30)            
             
        except requests.exceptions.Timeout:
            print("\n Requests Timeout")
            time.sleep(30)  

        except RateLimitExceededException:
            # Tiempo de espera segun la doc es de 1h
            print("\nRateLimitExceededException")
            print("Sleeping (1h)")
            time.sleep(3600)            
            # Re-connect ?
            g = Github(ACCESS_TOKEN)

    return df_temp


# Open file
fileKw = open("./keywordsAll.txt")

# Launch tasks
tasks = []
df_temp = pd.DataFrame()
with concurrent.futures.ThreadPoolExecutor(5) as executor:
    for kw in fileKw:       
        tasks.append(executor.submit(search_github_repos, kw))
            
    # Union
    for result in tasks:
        df_temp = df_temp.append(result.result())
        
print("End")
