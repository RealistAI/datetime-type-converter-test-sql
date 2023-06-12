from pathlib import Path
from utils import utils
from google.api_core import exceptions as gcp_exceptions
import pytest
import config
import datetime
import yaml
import csv
import os

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestCreateFailureLog:
    def test_ideal_conditions(self,failure_log_data):
        failure_log_path = f'{config.FAILURE_LOGS}/fake.csv'
        utils.create_failure_log(failure_log_path=failure_log_path,
                                 data=failure_log_data)

        with open (failure_log_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            count = 0
            for row in reader:
                if count == 0:
                    count += 1
                    pass
                elif count > 0:
                    file_name = row[0]
                    error_type = row[1]
                    error_message = row[2]
                    timestamp = row[3]

        assert file_name == failure_log_data['file_name']
        assert error_type == str(failure_log_data['error_type'])
        assert error_message == failure_log_data['error_message']
        assert timestamp == failure_log_data['timestamp']

        os.system(f'rm {failure_log_path}')

class TestWriteDataToCsvFile:
    def test_ideal_conditions(self, failure_log):
        header = ['file_name','error_type','error_message','timestamp']
        file_path = f'{config.FAILURE_LOGS}/fake.csv'
        utils.write_data_to_csv_file(file_path=file_path,
                                     header=header,
                                     row=failure_log)

        with open (file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            count = 0
            for row in reader:
                if count == 0:
                    count += 1
                    pass
                elif count > 0:
                    file_name = row[0]
                    error_type = row [1]
                    error_message = row[2]
                    timestamp = row[3]

        assert file_name == failure_log[0]
        assert error_type == str(failure_log[1])
        assert error_message == failure_log[2]
        assert timestamp == failure_log[3]

        os.system(f'rm {file_path}')


class TestFormatFailureLogData:
    def test_ideal_conditions(self, failure_log_data):
        formatted_data = utils.format_failure_log_data(data=failure_log_data)

        assert formatted_data[0] == failure_log_data['file_name']
        assert formatted_data[1] == failure_log_data['error_type']
        assert formatted_data[2] == failure_log_data['error_message']
        assert formatted_data[3] == failure_log_data['timestamp']

class TestCopyFile:
    def test_ideal_conditions(self,file):
        path_to_target = f'{config.FAILURE_LOGS}/copied_fake.csv'
        utils.copy_file(path_of_file_to_copy=file,
                        path_to_target=path_to_target)

        assert os.path.isfile(path_to_target)

        os.system(f'rm {path_to_target}')

class TestCreatePathIfNotExists:
    def test_if_not_exists(self):
        directory = f'{config.FAILURE_LOGS}fake_dir/'
        command = f'rm -rf {directory}'
        if os.path.isdir(directory):
            os.system(command)

        utils.create_path_if_not_exists(path=directory)

        assert os.path.isdir(directory)

        os.system(command)


class TestGetLatestFile():
    def test_ideal_conditions(self, files_for_numeric_comparison):
        file = utils.get_latest_file(config.FAILURE_LOGS)

        assert file == files_for_numeric_comparison

class TestRemoveNonAlphanumeric:
    def test_remove_non_alphanumeric(self):
        dirty_string = 'r3!m@03v$3d_a&1&(1_)n)0(n*_a1p!ha#n@u@m3!r$1^c^'
        clean_string = utils.remove_non_alphanumeric(string=dirty_string)

        assert clean_string == 'r3m03v3da11n0na1phanum3r1c'

@pytest.fixture
def failure_log_data():
    file_name = 'file.sql'
    message = 'A real error'
    error = gcp_exceptions.Conflict(message)
    error_type = type(error)
    timestamp = str(datetime.datetime.now())

    failure_log_data = {'file_name':file_name,
            'error_type':error_type,
            'error_message':message,
            'timestamp':timestamp}

    yield failure_log_data

@pytest.fixture
def failure_log(failure_log_data):

    failure_log = utils.format_failure_log_data(data=failure_log_data)

    yield failure_log

@pytest.fixture
def files_for_numeric_comparison():
    os.system(f'rm -rf {config.FAILURE_LOGS}/*')
    file_with_lesser_number = f'{config.FAILURE_LOGS}/4963.csv'
    file_with_greater_number = f'{config.FAILURE_LOGS}/4982.csv'
    with open (file_with_lesser_number, 'w') as file:
        file.write('I like that file.')
    with open (file_with_greater_number, 'w') as file:
        file.write('That\'s a nice file.')

    yield file_with_greater_number

    os.system(f'rm {file_with_lesser_number}')
    os.system(f'rm {file_with_greater_number}')


@pytest.fixture
def file():
    file_name = f'{config.FAILURE_LOGS}/fake.csv'
    with open (file_name, 'w') as file:
        file.write('File this')

    yield file_name

    os.system(f'rm {file_name}')

