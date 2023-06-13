import os
import pytest

pytest.fixture(scope='module')
def run_setup():
    os.system("python3 setup.py")
