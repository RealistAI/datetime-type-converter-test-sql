import config
import pytest
import logging
import time
import os
from pathlib import Path
from utils import utils, gcp
from google.cloud import bigquery

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestE2e:
    def test_e2e(self,
                 create_directories,
                 create_transpilation_log_table):
        # Clearing out local directories and buckets
        os.system('./prerun.sh')
        os.system(f'rm -rf ~/{config.BASE_PATH}/*')
        # Run make run to run the setup and main functions.
        os.system('make run')
        # assert transpiled and validated SQL are in the git repo
        time.sleep(5)
        assert is_repo_pushed(f'{config.BASE_PATH}/UC4_SQL/')
        assert is_table_populated(project_id=config.PROJECT, dataset_id=config.DATASET) != None
        print(f"{config.TARGET_SQL_PATH}/teradata_sql.sql")
        is_valid = os.path.isfile(f"{config.TARGET_SQL_PATH}/fst_with_whitlst_cnt_check.sql")
        assert is_valid == True
        is_valid2 = os.path.isfile(f"{config.TARGET_SQL_PATH}/radd_master_upd.sql")
        assert is_valid2 == True
        is_valid3 = os.path.isfile(f"{config.TARGET_SQL_PATH}/dw_table_current_roe.sql")
        assert is_valid3 == True

def is_table_populated(project_id,
                       dataset_id):
    data = f"""
           SELECT * FROM {config.PROJECT}.{config.DATASET}.transpilation_logs
           """
    try:
        query_job = submit_query(query=data,
                                 dry_run=False).results()
        return query_job

    except Exception as error:
        return error

def is_repo_pushed(repo_path):
    # Change to the repository directory
    repo_path = os.path.abspath(repo_path)
    os.chdir(repo_path)

    # Check if the repository is a valid Git repository
    if not os.path.isdir(os.path.join(repo_path, '.git')):
        raise ValueError(f"{repo_path} is not a Git repository.")

    # Iterate over local branches
    branches_output = os.popen('git for-each-ref --format=%(refname:short) refs/heads').read().strip()
    branches = branches_output.split('\n')

    for branch in branches:
        # Check if the branch has a corresponding remote branch
        tracking_branch_output = os.popen('git rev-parse --abbrev-ref --symbolic-full-name @{u}').read().strip()
        if tracking_branch_output:
            local_commits_output = os.popen(f'git rev-list --count {branch}').read().strip()
            remote_commits_output = os.popen(f'git rev-list --count {branch}..{tracking_branch_output}').read().strip()

            local_commits = local_commits_output
            remote_commits = remote_commits_output

            # Compare the number of commits between local and remote branches
            if local_commits > remote_commits:
                return False

    return True

@pytest.fixture(scope="session")
def create_directories():
    root = Path(os.getcwd())
    utils.create_path_if_not_exists(config.SOURCE_SQL_PATH)
    os.system(f"""
              cd {config.SOURCE_SQL_PATH};
              mkdir faster_withdrawal_whitelist;
              mkdir radd_master;
              mkdir dw_table_current;
              """)
    os.system(f'echo "SELECT * FROM \`michael-gilbert-dev.UC4_Jobs.uc4_to_sql_map\` LIMIT 1000" > {config.SOURCE_SQL_PATH}/faster_withdrawal_whitelist/fst_with_whitlst_cnt_check.sql')
    os.system(f'echo "SELECT job FROM \`michael-gilbert-dev.UC4_Jobs.uc4_to_sql_map\` LIMIT 1000" > {config.SOURCE_SQL_PATH}/radd_master/radd_master_upd.sql')
    os.system(f'echo "SELECT job FROM \`michael-gilbert-dev.UC4_Jobs.uc4_to_sql_map\` LIMIT 1000" > {config.SOURCE_SQL_PATH}/dw_table_current/dw_table_current_roe.sql')
    yield
    os.system("""
              cd ~/git/bq_migration_automation_tool;
              rm -r output/;
              rm empty_file.txt;
              """)

@pytest.fixture(scope="session")
def create_transpilation_log_table():
    client = bigquery.Client()
    try:
        create_table_query = client.query(f"""
                                          CREATE TABLE {config.PROJECT}.{config.DATASET}.transpilation_logs(
                                              job_id STRING,
                                              status STRING,
                                              message STRING,
                                              query STRING,
                                              run_time TIMESTAMP
                                          );""")

        results = create_table_query.result()

        for row in results:
            print(f"{row.url} : {row.view_count}")

    except Exception as error:
        print(error)

