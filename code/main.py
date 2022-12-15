import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Tuple


def create_session() -> SparkSession:
    """
    creates SparkSession
    """
    return SparkSession.builder.getOrCreate()


def read_data(spark: SparkSession,
              client_csv: str,
              financial_csv: str) -> Tuple[DataFrame, DataFrame]:
    """
    reads data from csv files into Spark DataFrames
    """
    clientDF = spark.read.csv(client_csv, header=True)
    financialDF = spark.read.csv(financial_csv, header=True)
    return clientDF, financialDF


def process_data(clientDF: DataFrame,
                 financialDF: DataFrame,
                 countries: list,
                 rename: dict) -> DataFrame:
    """
    Processes DataFrames, including filtering, merging and renaming
    """
    if countries:
        clientDF = clientDF.filter(
                clientDF.country.isin(countries)
            ).select(
                clientDF.id, clientDF.email, clientDF.country
            )
    else:
        clientDF = clientDF.select(
                clientDF.id, clientDF.email, clientDF.country
            )

    financialDF = financialDF.select(
            financialDF.id, financialDF.btc_a, financialDF.cc_t
        )

    df = clientDF.join(financialDF, ['id'])
    df = df.select([col(c).alias(rename.get(c, c)) for c in df.columns])
    return df


def output(df: DataFrame, folder: str):
    """
    Outputs data to csv
    """
    df.write.format('csv').mode('overwrite').options(header=True).save(folder)


def main(client_csv: str,
         financial_csv: str,
         countries: list,
         rename: dict = {
            'id': 'client_identifier',
            'btc_a': 'bitcoin_address',
            'cc_t': 'credit_card_type'
            },
         folder: str = 'client_data/'):
    """
    main
    """
    spark = create_session()
    clientDF, financialDF = read_data(spark, client_csv, financial_csv)
    df = process_data(clientDF, financialDF, countries, rename)
    output(df, folder=folder)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('client_csv')
    parser.add_argument('financial_csv')
    parser.add_argument('--countries', nargs='*', dest='countries', default=[])
    args = parser.parse_args()

    main(args.client_csv, args.financial_csv, args.countries)
