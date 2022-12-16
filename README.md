# PySpark - Assignment
This code is for an Assignment in PySpark (see exercise.md for details)

## Description
The code for this assignment can be found in code/pyspark_assignment.py. The program takes two datasets,
filters them and merges them. The output is saved in csv format to the client_data/ directory. 

## Installation
Clone the or download the latest version of this repository. The code has been tested using Python 3.7.15 and pyspark 3.2.1. You can use the `requirements.txt` file or `environment.yml` to setup a python environment. 

## Usage
The program can be ran from the command line using:
 
 `python code/main.py file1 file2 [--countries list] [--outdir directory]`

 The program takes two input files in csv format:
 * `file1` is expected to be the client data file with the necessary columns:
    * `id` client identifier
    * `email` client email address
    * `country` client country
 * `file2` is expected to be the financial details data file with the necessary columns:
    * `id` client identifier
    * `btc_a` bitcoin address
    * `cc_t` creditcard type (e.g. mastercard, visa)

The program also has two optional arguments:
* `--countries list` where the `list` is a list of countries on which the data is filtered
* `--outdir directory` where `directory` is the desired output directory.


Alternatively, one can import `main()` from `code/pyspark_assignment.py` and run the code directly in Python. See the docstring for details on usage.