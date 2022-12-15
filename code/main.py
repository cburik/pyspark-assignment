import argparse
from pyspark.sql import SparkSession, DataFrame
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


def main(client_csv, financial_csv):
    """
    main
    """
    spark = create_session()
    clientDF, financialDF = read_data(spark, client_csv, financial_csv)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('client_csv')
    parser.add_argument('financial_csv')
    args = parser.parse_args()
    
    main(args.client_csv, args.financial_csv)
