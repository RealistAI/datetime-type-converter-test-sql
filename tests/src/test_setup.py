import pytest
import config
import setup
import os

class TestSetup:
    def test_setup_successfully(self):
        os.system(f'rm -rf {config.BASE_PATH}/*')
        setup.setup()

        assert os.path.exists(f'{config.BASE_PATH}/UC4_SQL')
        assert os.path.exists(f'{config.BASE_PATH}/dwh-migration-tools')

