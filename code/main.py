import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Tuple


def create_session() -> SparkSession:
    """
    creates SparkSession
    """
    logging.debug('Starting SparkSession')
    return SparkSession.builder.getOrCreate()


def read_data(spark: SparkSession,
              client_csv: str,
              financial_csv: str) -> Tuple[DataFrame, DataFrame]:
    """
    reads data from csv files into Spark DataFrames
    """
    logging.info('Reading data from files: '
                 'client data: "{}"; '
                 'financial data: "{}"'.format(client_csv, financial_csv))

    clientDF = spark.read.csv(client_csv, header=True)
    financialDF = spark.read.csv(financial_csv, header=True)
    logging.info(
        "Number of lines in client data: {}".format(clientDF.count())
        )
    logging.info(
        "Number of lines in financial data: {}".format(financialDF.count())
        )
    return clientDF, financialDF


def process_data(clientDF: DataFrame,
                 financialDF: DataFrame,
                 countries: list,
                 rename: dict) -> DataFrame:
    """
    Processes DataFrames, including filtering, merging and renaming
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

    financialDF = financialDF.select(
            financialDF.id, financialDF.btc_a, financialDF.cc_t
        )

    logging.info('Merging datasets...')
    df = clientDF.join(financialDF, ['id'])
    df = df.select([col(c).alias(rename.get(c, c)) for c in df.columns])
    logging.info('Number of lines in final dataset: {}'.format(df.count()))
    return df


def output(df: DataFrame, outdir: str):
    """
    Outputs data to csv
    """
    df.write.format('csv').mode('overwrite').options(header=True).save(outdir)


def main(client_csv: str,
         financial_csv: str,
         countries: list,
         rename: dict = {
            'id': 'client_identifier',
            'btc_a': 'bitcoin_address',
            'cc_t': 'credit_card_type'
            },
         outdir: str = 'client_data/'):
    """
    main
    """
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        filename='logs/pyspark-assignment.log',
                        level=logging.INFO)
    logging.critical('Starting')

    spark = create_session()
    clientDF, financialDF = read_data(spark, client_csv, financial_csv)
    df = process_data(clientDF, financialDF, countries, rename)
    output(df, outdir=outdir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('client_csv')
    parser.add_argument('financial_csv')
    parser.add_argument('--countries', nargs='*', dest='countries', default=[])
    parser.add_argument('--outdir', dest='outdir', default='client_data/')
    args = parser.parse_args()

    main(args.client_csv, args.financial_csv, args.countries)
