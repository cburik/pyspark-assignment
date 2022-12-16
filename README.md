# PySpark - Assignment
This code is for an Assignment in PySpark (see exercise.md for details)

## Description
The code for this assignment can be found in code/pyspark_assignment.py. The program takes two datasets,
filters them and merges them. The output is saved in csv format to the client_data/ directory. 

## Installation
Clone the or download the latest version of this repository. The code has been tested using Python 3.7.15 and pyspark 3.2.1. You can use the `requirements.txt` file or `environment.yml` to setup a python environment. 

If you want to use it as a python module, clone this repository or download the latest distribution from `dist/` and install using `setup.py` or `pip install -e .`.

## Usage

### From Command Line
The program can be ran from the command line using:
 
 `python pyspark_assignment/pyspark_assignment.py file1 file2 [--countries list] [--outdir directory]`

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

### As Python module
Alternatively, if you have installed this as a python module one can run the following code.

      from pyspark_assignment import pyspark_assignment

      pyspark_assignment.main(
         'input/dataset_one.csv', 
         'input/dataset_two.csv', 
         ['United Kingdom', 'Netherlands'])
   

The function takes three arguments and three optional keyword arguments:

* `client_csv: str` path to csv file with client data with expected columns:
            * id: clientid number
            * email: client emailadress
            * country: client country
* `financial_csv: str` path to csv file with financial data with expected columns:
            * id: clientid number
            * btc_a: bitcoin address
            * cc_t: credit card type
* `countries: list` list of countries to filter on. If empty, country filter is skipped. Default: empty list. 
* `rename: dict` dictionary of columns to rename. Default: 

      {
         'id': 'client_identifier',
         'btc_a': 'bitcoin_address',
         'cc_t': 'credit_card_type'
         }
* `outdir: str` output directory, default: `'client_data/'`
* `logpath: str` log file to write logs to, default: `'logs/pyspark-assignment.log'`
