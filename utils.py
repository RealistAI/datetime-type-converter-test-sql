from collections.abc import Iterable

from typing import Dict, Tuple
import config
import datetime
from pathlib import Path
import os
import re
import json

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

def create_path_if_not_exists(path) -> None:
    """
    Create the file path if it does not exist

    Args:
    path: the file path we are creating if it doesn't exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)

def remove_non_alphanumeric(string):
    """ Removes all characters that are not numbers or letters

    Args:
    string: The string you wish to remove non alphanumeric characters from.
    """
    alphanumeric_chars = []
    for char in string:
        if char.isalnum():
            alphanumeric_chars.append(char)
    return ''.join(alphanumeric_chars)

def push_to_git(remote_repo,
                commit_message) -> str:
    """
    creates a unique branch name and pushes the changes to the GitHub Repository.

    Args:
    remote_repo: The GitHub Repository that the changes are being pushed to.
    commit_message: The commit message that is being pushed to the GitHub Repository alone with the necessary changes.
    """
    current_datetime = str(datetime.datetime.now())
    stripped_current_datetime = remove_non_alphanumeric(string=current_datetime)
    branch_name = f'bq_migration_tool_batch_{stripped_current_datetime}'
    base_path = config.BASE_PATH

    current_directory = os.getcwd()
    os.chdir(base_path)
    repo_directory_name = get_path_from_git_repo(remote_repo['path'])

    assert repo_directory_name is not None, \
        f"'{remote_repo['path']}' is not a valid git repo."

    if os.path.exists(Path(base_path, repo_directory_name)):
        os.chdir(repo_directory_name)
        os.system(f'git checkout -b {branch_name}')
        os.system(f'git add .')
        os.system(f'git commit -m "{commit_message}"')
        os.system(f'git push --set-upstream origin {branch_name}')

    os.chdir(current_directory)
    return branch_name


def get_path_from_git_repo(repo_dir: str) -> str | None:
    """
    Given a repo like https://github.com/RealistAI/UC4_SQL.git return
    UC4_SQL

    Args:
    repo_dir:  the full GitHub path of the repo that we are using to extract just the repository name.
    """
    match = re.search(r'[a-zA-Z-0-9_]*(?=\.git)', repo_dir)

    if match:
        return match.group()

    return None


def get_git_repo(repo: dict,
                 base_path: Path) -> None:
    """
    Given a repo name and a base path, get the latest version of the git repo
    If the repo is already downloaded to the current file system, pull the
    latest version

    Args:
    repo: Name of the GitHub repository you want to pull from
    base_path: The path leading to the GitHub Repository
    """
    current_dir = os.getcwd()
    os.chdir(base_path)

    repo_dir_name = get_path_from_git_repo(repo['path'])
    assert repo_dir_name is not None, \
        f"'{repo['path']}' is not a valid git repo."

    if os.path.exists(Path(base_path, repo_dir_name)):
        os.chdir(repo_dir_name)
        os.system(f"git checkout {repo['branch']}")
        os.system("git pull")
    else:
        os.system(f"git clone {repo['path']}")
        os.chdir(repo_dir_name)
        os.system(f"git checkout {repo['branch']}")

    os.chdir(current_dir)


def extract_sql_dependencies(sql_dependencies: list):
    """
    Recursively looks in the uc4 JSON for the `sql_dependencies` element, adds
    them to a list and returns them

    Args:
    sql_dependencies: The list containing the nested dictionaries which contain all the
                      sql_dependencies such as the sql_file_paths needed for transpilation and validation.
    """
    sql_paths = []
    for dependencies in sql_dependencies:
        if dependencies.get('sql_dependencies'):
            sql_paths.extend(extract_sql_dependencies(dependencies['sql_dependencies']))
        else:
            sql_file_path = dependencies.get("sql_file_path")
            sql_paths.append(sql_file_path)
    return list(set(sql_paths))

def get_uc4_json(client: bigquery.Client, uc4_job_name: str) -> Dict:
    """
    Runs a query to attain the json data located in the uc4_json table for a specific uc4_job.

    Args:
    project_id: the project_id used in conjunction with the dataset_id to access the table.
    dataset_id: the dataset_id used in conjunction with the project_id to access the table.
    uc4_job_name: the name of the uc4_job we want the json data from.
    """

    # get the json for this uc4 job from BigQuery
    json_data_query = "SELECT json_data\n"\
            f"FROM {config.UC4_JSON_TABLE}\n"\
            f"WHERE job_id = '{uc4_job_name}'"
    
    results = submit_query(client=client, query=json_data_query)

    # Convert the JSON to a Dict
    for row in results:
        json_data = row[0]
        dependency_dict = json.loads(json_data)

        # Return it 
        return dependency_dict


def submit_query(client: bigquery.Client, query:str) -> Iterable:
    """
    Submit a query to BigQuery
    """
    
    return client.query(query=query,
                        job_config=bigquery.QueryJobConfig()
                        ).result()

def submit_dry_run(client: bigquery.Client, query: str) -> Tuple[str, str]:
    """
    Submit a dry run to BigQuery. Return 'SUCEEDED' or 'FAILED' and any
    messages it returns
    """

    job_config = bigquery.QueryJobConfig(dry_run=True,
                                         use_query_cache=False)
    try:
        client.query(query, job_config=job_config)
    except BadRequest as e:
        return 'FAILED', e.message

    return 'SUCEEDED', ''


