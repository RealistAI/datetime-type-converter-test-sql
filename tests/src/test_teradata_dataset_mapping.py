from utils import utils
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Forbidden, BadRequest, ServiceUnavailable, Conflict, TooManyRequests
import teradata_to_bq_dataset_mapping as tdm
import config
import pytest


class TestDatasetMapping:
    def test_generate_table_mapping_success(self, create_data_mapping_table):
        dataset_mapping = tdm.generate_table_mapping(project_id=config.PROJECT, dataset_id=config.DATASET,
                                                     uc4_job_name='"UC4_JOB_1"', business_unit="A")
        assert type(dataset_mapping) == dict

    def test_generate_table_mapping_fail_due_to_invalid_job(self):
        with pytest.raises(TypeError):
            dataset_mapping = tdm.generate_table_mapping(project_id=config.PROJECT, dataset_id=config.DATASET,
                                                         uc4_job_name="UC4_JOB_7.5", business_unit="A")


@pytest.fixture(scope="session")
def create_data_mapping_table():
    client = bigquery.Client()

    try:
        create_query = client.query(f"""
                                    CREATE TABLE {config.PROJECT}.{config.DATASET}.dataset_mapping(
                                    table_mapping_DDL STRING, table_mapping_DML STRING)
                                    """);

        results = create_query.result()
        print(f"{results} uploaded to dataset_mapping table")
        return results

    except Exception as error:
        print(error)
