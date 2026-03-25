import pandas as pd
import numpy as np
import logging
import argparse
import json
import os
import sys


#============================================================
# 1. Implementing Argument Parser for Command Line Interface
#============================================================
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


