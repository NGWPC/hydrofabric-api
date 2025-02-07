import os
import shutil
import pytest

# Clean up any leftover debugging IPE results files 
def pytest_sessionfinish(session, exitstatus):
    if exitstatus == 0:
        results_debugging_dir_path = f"{os.path.dirname(__file__)}/resources/.actual/"
        if os.path.exists(results_debugging_dir_path):
            shutil.rmtree(results_debugging_dir_path)
