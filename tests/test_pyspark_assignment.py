import pytest
import logging
import glob
import os
from pyspark.sql import SparkSession
from pyspark_assignment import pyspark_assignment
from chispa import dataframe_comparer

spark = SparkSession.builder.getOrCreate()


@pytest.fixture
def clientDF_expected():
    df = spark.read.csv('tests/fixtures/clients.csv', header=True)
    return df


@pytest.fixture
def financialDF_expected():
    df = spark.read.csv('tests/fixtures/financials.csv', header=True)
    return df


@pytest.fixture
def financialDF_unbalanced():
    df = spark.read.csv('tests/fixtures/financials_unbalanced.csv',
                        header=True)
    return df


@pytest.fixture
def outputDF_balanced():
    df = spark.read.csv('tests/fixtures/output_balanced.csv',
                        header=True)
    return df


@pytest.fixture
def outputDF_unbalanced():
    df = spark.read.csv('tests/fixtures/output_unbalanced.csv',
                        header=True)
    return df


@pytest.fixture
def outputDF_filtered():
    df = spark.read.csv('tests/fixtures/output_filtered.csv',
                        header=True)
    return df


@pytest.fixture
def outputDF_main():
    df = spark.read.csv('tests/fixtures/output_main.csv',
                        header=True)
    return df


def test_create_session():
    """tests if a SparkSession is created"""
    spark = pyspark_assignment.create_session()
    assert type(spark) == SparkSession


def test_read_data_balanced(clientDF_expected, financialDF_expected):
    """Tests if data is read into a Spark DataFrame
     correctly using the example data"""
    clientDF, financialDF = pyspark_assignment.read_data(
                                        spark,
                                        'tests/fixtures/clients.csv',
                                        'tests/fixtures/financials.csv')
    dataframe_comparer.assert_df_equality(clientDF, clientDF_expected)
    dataframe_comparer.assert_df_equality(financialDF, financialDF_expected)


def test_read_data_unbalanced(caplog):
    """Tests if unbalanced data gives a warning"""
    with caplog.at_level(logging.WARNING):
        clientDF, financialDF = pyspark_assignment.read_data(
                                    spark,
                                    'tests/fixtures/clients.csv',
                                    'tests/fixtures/financials_unbalanced.csv')

    assert 'Unequal number of rows in DataFrames' in caplog.text


def test_process_data_balanced(clientDF_expected,
                               financialDF_expected,
                               outputDF_balanced
                               ):
    """Tests if process data runs with minimal options"""
    df = pyspark_assignment.process_data(clientDF_expected,
                                         financialDF_expected,
                                         [], rename={})
    dataframe_comparer.assert_df_equality(df, outputDF_balanced)


def test_process_data_unbalanced(caplog,
                                 clientDF_expected,
                                 financialDF_unbalanced,
                                 outputDF_unbalanced
                                 ):
    """Tests if process data runs with data missing from financials"""
    with caplog.at_level(logging.WARNING):
        df = pyspark_assignment.process_data(clientDF_expected,
                                             financialDF_unbalanced,
                                             [], rename={})
    dataframe_comparer.assert_df_equality(df, outputDF_unbalanced)
    assert 'Number of lines in client' in caplog.text


def test_process_data_filtering(clientDF_expected,
                                financialDF_expected,
                                outputDF_filtered
                                ):
    """Tests if filtering in process data"""
    df = pyspark_assignment.process_data(clientDF_expected,
                                         financialDF_expected,
                                         ['United Kingdom',
                                          'Netherlands'],
                                         rename={})
    dataframe_comparer.assert_df_equality(df, outputDF_filtered)


def test_process_data_renaming(clientDF_expected,
                               financialDF_expected,
                               ):
    """Tests if filtering in process data"""
    df = pyspark_assignment.process_data(clientDF_expected,
                                         financialDF_expected,
                                         [],
                                         rename={'btc_a': 'bitcoin'})
    assert 'bitcoin' in df.columns


def test_output(tmpdir, clientDF_expected):
    """Tests if output function creates csv file"""
    tmp_path = tmpdir.strpath
    pyspark_assignment.output(clientDF_expected, tmp_path)
    assert len(glob.glob(os.path.join(tmp_path, '*csv'))) > 0


def test_main(tmpdir, outputDF_main):
    """Tests full output of main"""
    out_path = os.path.join(tmpdir.strpath, 'output')
    os.mkdir(out_path)
    log_path = os.path.join(tmpdir.strpath, 'mylog.log')
    pyspark_assignment.main('tests/fixtures/clients.csv',
                            'tests/fixtures/financials.csv',
                            ['United Kingdom',
                             'Netherlands'],
                            outdir=out_path,
                            logpath=log_path
                            )
    outfile = glob.glob(os.path.join(out_path, '*.csv'))
    result = spark.read.csv(outfile[0], header=True)
    dataframe_comparer.assert_df_equality(result, outputDF_main)
