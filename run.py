import pandas as pd
import csv
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
    required_keys = ['seed', 'window_size', 'version']
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
        # Forcing pandas to ignore quotes entirely when splitting
        df = pd.read_csv(input_path, quoting=csv.QUOTE_NONE)

        # Clean the column headers (remove leftover quotes and spaces)
        df.columns = df.columns.str.replace('"', '').str.strip().str.lower()

        # Clean the first and last column's data (they have leftover quotes too)
        df['timestamp'] = df['timestamp'].astype(str).str.replace('"', '')
        df['volume_usd'] = df['volume_usd'].astype(str).str.replace('"', '')

        # Ensure our 'close' column is strictly numerical for the math
        df['close'] = pd.to_numeric(df['close'], errors='coerce')

        # Checking for the missing required column
        if 'close' not in df.columns:
            raise ValueError("Data validation failed: 'close' column is missing.")

        return df

    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found at path: {input_path}")
    except pd.errors.EmptyDataError:
        raise ValueError(f"The provided dataset at {input_path} is completely empty.")
    except pd.errors.ParserError:
        raise ValueError(f"The file at {input_path} is not a valid CSV format.")



#===============================================================
# 4. Calculating Rolling Mean and Signal
#===============================================================
def process_data(df, window_size):
    # Calculating the rolling mean
    df['rolling_mean'] = df['close'].rolling(window=window_size).mean()

    # Generate the binary signal
    df['signal'] = (df['close'] > df['rolling_mean']).astype(int)

    return df

config = parse_config_file(args.config)
df = load_and_validate_data(args.input)
df = process_data(df, config['window_size'])
print(df.head())



#===============================================================
# 5. Creating Function for Metric.json
#===============================================================
def create_metrics(df, start_time, end_time, config, output_path):
    Metrics_dict = {
         "version": config['version'],
        "rows_processed":df.shape[0],
        "metric": "signal_rate",
        "value": df['signal'].mean(),
        "latency_ms": int((end_time - start_time) * 1000),
        "seed": config['seed'],
        "status": "success"
    }
    with open(output_path, 'w') as file:
        json.dump(Metrics_dict, file, indent=4)

def write_error_metrics(error_message, output_path):
    Metrics_dict = {
        "version": config['version'],
        "status": "error",
        "error_message": error_message,
    }
    with open(output_path, 'w') as file:
        json.dump(Metrics_dict, file, indent=4)