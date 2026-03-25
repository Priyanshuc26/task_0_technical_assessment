import pandas as pd
import numpy as np
import logging
import argparse
import yaml
import json
import os
import sys

#===============================================================
# 1. Implementing Custom Exception Class for Exception Handling
#===============================================================
# class CustomException (Exception):
#     def __init__(self, error_message, error_details:sys):
#         self.error_message = error_message
#
#         _, _,  exc_tb = error_details.exc_info()
#         self.lineno=exc_tb.tb_lineno
#         self.file_name=exc_tb.tb_frame.f_code.co_filename
#
#     def _str_(self):
#         return "Error occurred in python script name [{0}] line number [{1}] error message [{2}]".format(self.file_name, self.lineno, str(self.error_message))



#===============================================================
# 1. Implementing Argument Parser for Command Line Interface
#===============================================================
parser = argparse.ArgumentParser(
    description="Minimal MLOps batch job for calculating rolling means."
)

# Defining arguments that is required in CLI
parser.add_argument("--input", required=True, help="Takes csv file as input. Define csv file name")
parser.add_argument("--config", required=True, help="Takes Config file as input. Define config file name")
parser.add_argument("--output", required=True, help="Gives output as metrics in form of json file. Define json file name")
parser.add_argument("--log-file", required=True, help="Gives log file as output on successful run. Define log file name")

# Parsing arguments
args = parser.parse_args()
print(args)



#===============================================================
# 2. Configuration Loading and Validation
#===============================================================
def parse_config_file(config_path):
    with open(config_path, 'r') as file:
        yaml_config = yaml.safe_load(file)

    # Checking if the required keys exist in the dictionary
    required_keys = ['seed', 'window', 'version']
    for key in required_keys:
        if key not in yaml_config:
            raise ValueError(f"Config validation failed: '{key}' is missing")

    return yaml_config

# print(parse_config_file(config_path=args.config))



#===============================================================
# 3. Data Loading and Validation
#===============================================================
def load_and_validate_data(input_path):
    try:
        df = pd.read_csv(input_path)

        # Checking for the missing required column 'closing'
        if 'close' not in df.columns:
            raise ValueError("Data validation failed: 'close' column is missing.")

        return df

    # Catching the missing file error
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found at path: {input_path}")

    # Catching the empty file error
    except pd.errors.EmptyDataError:
        raise ValueError(f"The provided dataset at {input_path} is completely empty.")

    # Catching the invalid CSV format error
    except pd.errors.ParserError:
        raise ValueError(f"The file at {input_path} is not a valid CSV format.")


