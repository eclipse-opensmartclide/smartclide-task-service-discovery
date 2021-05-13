import pandas as pd
import time
import calendar
from datetime import datetime

from github import Github
from github import RateLimitExceededException

ACCESS_TOKEN = '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'  # Github persoal API
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
                #count += 1
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

            df_temp = df_temp.reset_index(drop=True)
            # One file per keyword
            df_temp.to_csv(r'C:/Users/David/Desktop/Github/' + 'GitHub_' + ','.join(keywords) +
                           '_' + datetime.now().strftime('%d_%m_%Y') + '.csv', index=True, header=True)
            raise StopIteration

        except StopIteration:
            break

        except RateLimitExceededException:
            # Normalmente es 1h

            print("\nRateLimitExceededException")
            search_rate_limit = g.get_rate_limit().search
            reset_timestamp = calendar.timegm(
                search_rate_limit.reset.timetuple())
            sleep_time = reset_timestamp - calendar.timegm(time.gmtime()) + 10
            print("Sleeping (s): ", sleep_time * 1200)

            time.sleep(3600)

            g = Github(ACCESS_TOKEN)

    return df_temp


def search_github_files(keywords, extension):

    # Split keywords
    keywords = [keyword.strip() for keyword in keywords.split(',')]

    # The API call limit is checked.
    rate_limit = g.get_rate_limit()
    rate = rate_limit.search
    if rate.remaining == 0:
        print(
            f'You have 0/{rate.limit} API calls remaining. Reset time: {rate.reset}')
        return
    else:
        print(f'You have {rate.remaining}/{rate.limit} API calls remaining')

    # The query is set
    query = f'"{keywords} english" in:file extension:' + extension  # *.po
    # Search query
    result = g.search_code(query, order='desc')

    # The list of repositories containing the keywords is printed out
    # in the file extension
    max_size = 100
    print(f'Found {result.totalCount} file(s)')

    # max 100
    if result.totalCount > max_size:
        result = result[:max_size]

    # print
    for file in result:
        print(f'{file.download_url}')


df_github = pd.DataFrame()

fileKw = open("C:/Users/David/Desktop/keywordsAll.txt")
for kw in fileKw:
    print("Keyword: ", kw.rstrip('\n'))
    df_temp = search_github_repos(kw)
    df_github = df_github.append(df_temp).reset_index(drop=True)

df_github.to_csv(r'C:/Users/David/Desktop/Github/' + 'GitHub_All_' +
                 datetime.now().strftime('%d_%m_%Y') + '.csv', index=True, header=True)

#search_github_files("", ".wsdl")
# search_github_repos(keywords)
