import pandas as pd
import csv
import numpy as np
import logging
import argparse
import yaml
import json
import os
import sys
import time
from datetime import datetime


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
# args = parser.parse_args()
# print(args)



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

        # Cleaning the column headers by removing leftover quotes and spaces
        df.columns = df.columns.str.replace('"', '').str.strip().str.lower()

        # Cleaning the first and last column's data as they have leftover quotes
        df['timestamp'] = df['timestamp'].astype(str).str.replace('"', '')
        df['volume_usd'] = df['volume_usd'].astype(str).str.replace('"', '')

        # Ensuring 'close' column is strictly numerical
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

# config = parse_config_file(args.config)
# df = load_and_validate_data(args.input)
# df = process_data(df, config['window_size'])
# print(df.head())



#===============================================================
# 5. Creating Function for Metric.json
#===============================================================
def create_metrics(df, start_time, end_time, config, output_path):
    metrics_dict = {
         "version": config['version'],
        "rows_processed":df.shape[0],
        "metric": "signal_rate",
        "value": float(df['signal'].mean()),
        "latency_ms": int((end_time - start_time) * 1000),
        "seed": config['seed'],
        "status": "success"
    }
    with open(output_path, 'w') as file:
        json.dump(metrics_dict, file, indent=4)
        
    return(json.dumps(metrics_dict, indent=4))
        


def write_error_metrics(error_message, output_path):
    metrics_dict = {
        "version": "v1",
        "status": "error",
        "error_message": error_message,
    }
    with open(output_path, 'w') as file:
        json.dump(metrics_dict, file, indent=4)
    
    return(json.dumps(metrics_dict, indent=4))



# ===============================================================
# 6. Logging Configuration
# ===============================================================
def setup_custom_logger(log_file_path):
    # Custom format: [Timestamp] - [Level] - [Message]
    log_format = "%(asctime)s - %(levelname)s - %(message)s"

    # Configuring the root logger
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # console_handler = logging.StreamHandler(sys.stdout)
    # console_handler.setFormatter(logging.Formatter(log_format))
    # logging.getLogger().addHandler(console_handler)





# ===============================================================
# Setting up the Pipeline
# ===============================================================
if __name__ == "__main__":
    # 1. Parse arguments
    args = parser.parse_args()

    # 2. Starting the custom logger using the CLI argument
    setup_custom_logger(args.log_file)
    logging.info(f"Starting MLOps batch processing job at {datetime.now()}")

    try:
        # Start time
        start_time = time.time()

        # Load Config
        logging.info(f"Loading configuration from {args.config}")
        config_data = parse_config_file(args.config)
        logging.info(f"Config loaded and validated. Configurations are: {config_data}")

        # Set Seed
        logging.info(f"Setting random seed to {config_data['seed']}")
        np.random.seed(config_data['seed'])

        # Load Data
        logging.info(f"Loading data from {args.input}")
        df = load_and_validate_data(args.input)
        logging.info(f"Successfully loaded {df.shape[0]} rows.")

        # Process Data
        logging.info(f"Processing data with rolling window of {config_data['window_size']}")
        df = process_data(df, config_data['window_size'])

        # End Time
        end_time = time.time()
        metrics_summary = create_metrics(df, start_time, end_time, config_data, args.output)
        print(metrics_summary)
        logging.info(f"Metrics summary: {metrics_summary}")
        logging.info(f"Job ended. status: sucess")

    except Exception as e:
        error_metric_summary = write_error_metrics(str(e), args.output)
        print(error_metric_summary)
        logging.error(f"Job failed. Error Metrics summary: {error_metric_summary}")
        sys.exit(1) # Force exit with non zero, so that if any error occurs, Docker knows that it is failed