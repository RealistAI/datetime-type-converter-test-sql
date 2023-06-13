from google.cloud import bigquery
import config
import logging
import transpilation_logs as tl
import datetime
import pytest
from google.api_core.exceptions import NotFound, Forbidden, BadRequest, ServiceUnavailable, Conflict, TooManyRequests

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestTranspilationLogs:
    def test_transpile_logs_into_table_with_success_data(self, create_transpilation_log_table, delete_table):
        current_datetime = str(datetime.datetime.now())
        insert_values =  tl.transpile_logs_into_table(project_id=config.PROJECT, dataset_id="merriks_dataset", job_id="uc4_test_job_1", status="SUCCEEDED", message="null", query="null", run_time=current_datetime)
        assert insert_values != Exception

    def test_transpile_logs_into_table_with_fail_data(self):
        current_datetime = str(datetime.datetime.now())
        insert_values =  tl.transpile_logs_into_table(project_id=config.PROJECT, dataset_id="merriks_dataset", job_id="uc4_test_job_2", status="FAILED", message="Expected keyword FROM but got NOT at [1:9]", query="SELECT * NOT LIMIT 1000", run_time=current_datetime)
        assert insert_values != Exception


    def test_transpile_logs_into_table_failed_due_to_nonexistent_dataset_id(self):
        with pytest.raises(Exception):
            tl.transpile_logs_into_table(project_id=config.PROJECT, dataset_id="not a real datatset", job_id="uc4_test_job_1", status="SUCCEEDED", message="null", query="null", run_time=current_datetime)

@pytest.fixture(scope="session")
def delete_table():
    client = bigquery.Client()
    yield
    try:
        delete_table = client.query(f"""
                                    DROP TABLE {config.PROJECT}.merriks_dataset.transpilation_logs;
                                    """
                                    )
        results = delete_table.result()
        logger.info(results)
    except Exception as error:
        logger.info(f"Error is {error}")

@pytest.fixture(scope="session")
def create_transpilation_log_table():
    client = bigquery.Client()
    try:
        create_table_query = client.query(f"""
                                          CREATE TABLE {config.PROJECT}.merriks_dataset.transpilation_logs(
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

