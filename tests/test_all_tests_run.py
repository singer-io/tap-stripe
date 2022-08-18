import unittest
import os

from tap_tester import LOGGER

potential_paths = [
    'tests/',
    '../tests/'
    'tap-stripe/tests/',
    '../tap-stripe/tests/',
]


def go_to_tests_directory():
    for path in potential_paths:
        if os.path.exists(path):
            os.chdir(path)
            return os.getcwd()
    raise NotImplementedError("This check cannot run from {}".format(os.getcwd()))

##########################################################################
### TEST
##########################################################################

class TestingTests(unittest.TestCase):
    def test_regression_suite(self):
        LOGGER.info("Acquiring path to tests directory.")
        cwd = go_to_tests_directory()

        LOGGER.info("Reading in filenames from tests directory.")
        files_in_dir = os.listdir(cwd)

        LOGGER.info("Dropping files that are not of the form 'test_<feature>.py'.")
        test_files_in_dir = [fn for fn in files_in_dir if fn.startswith('test_') and fn.endswith('.py')]

        LOGGER.info("Files found: %s", test_files_in_dir)

        LOGGER.info("Reading contents of circle config.")
        with open(cwd + "/../.circleci/config.yml", "r") as config:
            contents = config.read()

        LOGGER.info("Parsing circle config for run blocks.")
        runs = contents.replace(' ', '').replace('\n', '').split('-run_integration_test:')

        LOGGER.info("Verify all test files are executed in circle...")
        tests_not_found = set(test_files_in_dir)
        for filename in test_files_in_dir:
            LOGGER.info("\tVerifying %s is running in circle.", filename)
            trimmed_file_name = filename.split('.')[0].split('test_')[1]
            file_param = f"file:{trimmed_file_name}"
            if any([file_param in run for run in runs]):
                tests_not_found.remove(filename)
        self.assertSetEqual(tests_not_found, set())
        LOGGER.info("\t SUCCESS: All tests are running in circle.")

