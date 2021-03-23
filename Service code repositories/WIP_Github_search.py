
from github import Github

ACCESS_TOKEN = 'Redacted' # Github persoal API
g = Github(ACCESS_TOKEN)

# We look at the readme and the project description.
def search_github_repos(keywords):

    # Split keywords
    keywords = [keyword.strip() for keyword in keywords.split(',')]
    # The query is set
    query = '+'.join(keywords) + '+in:readme+in:description'
    result = g.search_repositories(query, 'stars', 'desc')
 
    print(f'Found {result.totalCount} repo(s)')
 
    for repo in result:
        print(f'{repo.clone_url}, {repo.stargazers_count} stars')


def search_github_files(keywords, extension):

    # Split keywords	
    keywords = [keyword.strip() for keyword in keywords.split(',')]
    
    # The API call limit is checked.
    rate_limit = g.get_rate_limit()
    rate = rate_limit.search
    if rate.remaining == 0:
        print(f'You have 0/{rate.limit} API calls remaining. Reset time: {rate.reset}')
        return
    else:
        print(f'You have {rate.remaining}/{rate.limit} API calls remaining')

    # The query is set
    query = f'"{keywords} english" in:file extension:' + extension  # *.po
    # Search query
    result = g.search_code(query, order='desc')

    # The list of repositories containing the keywords is printed out. 
    # in the file extension extension
    max_size = 100
    print(f'Found {result.totalCount} file(s)')

    # max 100
    if result.totalCount > max_size:
        result = result[:max_size]

    # print
    for file in result:
        print(f'{file.download_url}')

# Example
search_github_files("", ".wsdl")
search_github_repos(".wsdl")

# TODO> dataframes