from utils import gcp
import pytest
import sort_queries as s
import config
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestSortQueries:
    def test_sort_queries_successfully(self, create_directories):
        sort_queries = s.sort_queries(project_id=config.PROJECT, dataset_id=config.DATASET)
        assert type(sort_queries) == type(list())

    def test_sort_queries_fail_due_to_non_existent_project(self):
        with pytest.raises(Exception):
            s.sort_queries(project_id="not a real project", dataset_id=config.DATASET)

@pytest.fixture(scope="session")
def create_directories():
    os.system("""
              mkdir output;
              cd output;
              mkdir bteq;
              cd bteq;
              mkdir BU;
              cd BU;
              mkdir SIMBA;
              cd SIMBA;
              mkdir AMPS;
              cd AMPS;
              echo "SELECT * FROM master_table" > sql_1.sql;
              echo "SELECT job_id FROM master_table" > sql_2.sql;
              echo "SELECT current_year FROM master_table" > sql_3.sql;
              echo "SELECT * FROM second_table" > sql_4.sql;
              echo "SELECT sum_total FROM second_table" > sql_5.sql;
              echo "SELECT * FROM third_table" > sql_6.sql;
              echo "SELECT employee_id FROM fourth_table" > sql_7.sql;
              echo "SELECT current_month FROM fourth_table" > sql_8.sql;
              echo "SELECT * FROM final_table" > sql_9.sql;
              echo "SELECT taxe_id FROM final_table" > sql_10.sql;
              echo "SELECT orders FROM final_table" > sql_11.sql;
              """)
    yield
    os.system("rm -r output/")

