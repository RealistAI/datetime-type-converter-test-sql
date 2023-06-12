import os
import config
from pathlib import Path
import logging
import pytest
from utils import git as git

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestGit:
    def test_push_to_git_successfully(self):
        commit_message = 'Adding transpiled and validated GoogleSQL to the repository'
        branch_name = git.push_to_git(remote_repo=config.UC4_SQL_REPO,
                                      commit_message=commit_message)
        assert branch_name != None

    def test_push_to_git_fail_due_to_non_existent_repo(self):
        commit_message = "basic commit message"
        with pytest.raises(Exception):
            git.push_to_git(remote_repo="not a real repository",
                            commit_message=commit_message)

    def test_get_path_from_git_repo_successfully(self):
        repo_name = "https://github.com/RealistAI/bq_migration_automation_tool.git"
        get_path = git.get_path_from_git_repo(repo_dir=repo_name)
        assert get_path  == "bq_migration_automation_tool"

    def test_get_path_from_git_repo_fail_due_to_invalid_path(self):
        with pytest.raises(Exception):
            get_path = git.get_path_from_git_repo(repo_dir=not_valid)

    def test_get_repo_successfully(self):
        repo = config.UC4_SQL_REPO
        base_path = config.BASE_PATH
        get_repo = git.get_git_repo(repo=repo,
                                    base_path=base_path)
        assert get_repo == None

    def test_get_repo_fail_due_to_invalid_repo_name(self):
        repo = "not_a_real_repo_name"
        base_path = config.BASE_PATH
        with pytest.raises(Exception):
             get_repo = git.get_git_repo(repo=repo,
                                         base_path=base_path)


