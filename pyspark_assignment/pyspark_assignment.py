import argparse
import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Tuple


def logging_setup(filepath: str):
    """
    Setup of python logging module

    Uses a rotating logfile of max 20000 bytes, has 10 backups

    Parameters
    ----------
    filepath: str
        path to logfile

    Returns
    -------
    None
    """
    handler = RotatingFileHandler(filepath, maxBytes=20000, backupCount=10)
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        level=logging.INFO,
                        handlers=[handler])


def create_session() -> SparkSession:
    """
    creates SparkSession

    Parameters
    ----------
    None

    Returns
    -------
    SparkSession
    """
    logging.debug('Starting SparkSession')
    return SparkSession.builder.getOrCreate()


def read_data(spark: SparkSession,
              client_csv: str,
              financial_csv: str) -> Tuple[DataFrame, DataFrame]:
    """
    reads data from csv files into Spark DataFrames

    Parameters
    ----------
    spark: SparkSession
        SparkSession Object
    client_csv: str
        path to csv file with client data
    financial_csv: str
        path to csv file with financial data

    Returns
    -------
    Tuple containing two Spark DataFrames corresponding to the input files
    """
    logging.info('Reading data from files: '
                 'client data: "{}"; '
                 'financial data: "{}"'.format(client_csv, financial_csv))

    clientDF = spark.read.csv(client_csv, header=True)
    financialDF = spark.read.csv(financial_csv, header=True)
    client_count = clientDF.count()
    logging.info(
        "Number of lines in client data: {}".format(clientDF.count())
        )
    logging.info(
        "Number of lines in financial data: {}".format(financialDF.count())
        )
    fin_count = financialDF.count()
    if client_count != fin_count:
        logging.warning(
            'Unequal number of rows in DataFrames, client rows ({}) != '
            'financial rows ({})'.format(client_count, fin_count)
        )
    return clientDF, financialDF


def process_data(clientDF: DataFrame,
                 financialDF: DataFrame,
                 countries: list,
                 rename: dict) -> DataFrame:
    """
    Processes DataFrames, including filtering, merging and renaming

    Parameters
    ----------
    clientDF: DataFrame
        Spark DataFrame with expected columns:
            * id: clientid number
            * email: client emailadress
            * country: client country
    financialDF: DataFrame
        Spark DataFrame with expected columns:
            * id: clientid number
            * btc_a: bitcoin address
            * cc_t: credit card type
    countries: list
        list with countries to filter on,
        if list is empty the filter is not applied
    rename: dict
        dictionary with columns to rename

    Returns
    -------
    DataFrame
        processed Spark DataFrame (filtered, merged and renamed columns)
    """
    logging.info('Filtering data...')
    if countries:
        logging.info('Filtering on Countries: {}'.format(countries))
        clientDF = clientDF.filter(
                clientDF.country.isin(countries)
            ).select(
                clientDF.id, clientDF.email, clientDF.country
            )
    else:
        logging.info('Country list is empty, skipping country filter')
        clientDF = clientDF.select(
                clientDF.id, clientDF.email, clientDF.country
            )

    client_count = clientDF.count()
    logging.info('Number of lines after filtering: {}'.format(client_count))

    financialDF = financialDF.select(
            financialDF.id, financialDF.btc_a, financialDF.cc_t
        )

    logging.info('Merging datasets...')
    df = clientDF.join(financialDF, ['id'])
    df = df.select([col(c).alias(rename.get(c, c)) for c in df.columns])

    final_count = df.count()
    logging.info('Number of lines in final dataset: {}'.format(final_count))

    if client_count != final_count:
        logging.warning(
            'Number of lines in client data after filtering ({}), '
            'not equal to final dataset ({})'.format(client_count, final_count)
            )
    return df


def output(df: DataFrame, outdir: str):
    """
    Saves Spark DataFrame to csv

    Parameters
    ----------
    df: DataFrame
        A Spark DataFrame
    outdir: str
        The output directory

    Returns
    -------
    None
    """
    logging.info("Writing to directory: {}".format(outdir))
    df.write.format('csv').mode('overwrite').options(header=True).save(outdir)


def main(client_csv: str,
         financial_csv: str,
         countries: list,
         rename: dict = {
            'id': 'client_identifier',
            'btc_a': 'bitcoin_address',
            'cc_t': 'credit_card_type'
            },
         outdir: str = 'client_data/',
         logpath: str = 'logs/pyspark-assignment.log'):
    """
    Main function for dataprocessing

    This is the main function that handles the dataprocessing. It handles
    setup, reads data from csv, processes data and outputs data to csv.

    Parameters
    ----------
    client_csv: str
        path to csv file with client data with expected columns:
            * id: clientid number
            * email: client emailadress
            * country: client country
    financial_csv: str
        path to csv file with financial data with expected columns:
            * id: clientid number
            * btc_a: bitcoin address
            * cc_t: credit card type
    countries: list
        list of countries to filter on
    rename: dict
        dictionary of columns to rename, default:
            {
            'id': 'client_identifier',
            'btc_a': 'bitcoin_address',
            'cc_t': 'credit_card_type'
            }
    outdir: str
        Output directory, default: 'client_data/'
    logpath: str
        log file to write logs to, default: 'logs/pyspark-assignment.log'

    Returns
    -------
    None
    """
    logging_setup(logpath)
    logging.critical('Starting')

    spark = create_session()
    clientDF, financialDF = read_data(spark, client_csv, financial_csv)
    df = process_data(clientDF, financialDF, countries, rename)
    output(df, outdir=outdir)
    logging.info("Finished")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('client_csv')
    parser.add_argument('financial_csv')
    parser.add_argument('--countries', nargs='*', dest='countries', default=[])
    parser.add_argument('--outdir', dest='outdir', default='client_data/')
    parser.add_argument('--logpath', dest='outdir',
                        default='logs/pyspark-assignment.log')

    args = parser.parse_args()

    main(args.client_csv, args.financial_csv, args.countries,
         outdir=args.outdir, logpath=args.logpath)
