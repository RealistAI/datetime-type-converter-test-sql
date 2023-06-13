import pytest
from google.cloud import bigquery
from google.cloud import storage
import logging
import os
from utils import gcp
from pathlib import Path
import config

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestGCP:
    def test_validate_sql_successfully(self, run_setup):
        files_to_ignore = ['batch_translation_report.csv','consumed_name_map.json']
        os.system(f'mv {config.SQL_TO_VALIDATE}/batch_translation_report.csv translation_reports')
        for file_name in os.listdir(config.SQL_TO_VALIDATE):
            if file_name not in files_to_ignore:
                logger.info(f'Validating {file_name}')
                sql_file_to_validate = f'{config.SQL_TO_VALIDATE}/{file_name}'
                is_valid = gcp.validate_sql(sql_to_validate=sql_file_to_validate,
                                                  file_name=file_name)
        assert is_valid == True

    def test_validate_sql_fail_due_to_invalid_sql(self):
        file_name = "not_a_real_file.txt"
        sql_to_validate = f'{config.SQL_TO_VALIDATE}/{file_name}'
        with pytest.raises(Exception):
            gcp.validate_sql(sql_to_validate=sql_to_validate,
                             file_name=file_name)

#    def test_submit_query_successfully(self):
#        submut_query = gcp.submit_query(query="""SELECT * FROM MyTable LIMIT 10""",
#                                        dry_run =

#    def test_submit_query_failed_due_to_invalid_query(self):
#        pass


@pytest.fixture
def run_setup():
    os.system('python setup.py')
