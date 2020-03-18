
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType

from utils.trec_car_tools import iter_pages, Para, ParaBody, ParaText, ParaLink, Section, Image, List
from parse_trec_car import parse_page
import spacy
import time
import json
import six


# processing pyspark job
def run_job(read_path, write_path, num_pages=1, print_intervals=100, write_output=False):
    """ Runs processing job - reads TREC CAR cbor file and writes new file with improved entity linking """
    spark = SparkSession.builder.appName('trec_car').getOrCreate()
    spacy_nlp = spacy.load("en_core_web_sm")
    t_start = time.time()
    with open(read_path, 'rb') as f:
        for i, page in enumerate(iter_pages(f)):

            # stops when 'num_pages' processed
            if i >= num_pages:
                break

            # build PySpark DataFrame
            if i == 0:
                df = parse_page(page=page, i=i, spark=spark, spacy_nlp=spacy_nlp)
            else:
                new_row = parse_page(page=page, i=i, spark=spark, spacy_nlp=spacy_nlp)
                df = df.union(new_row)

            if (i % print_intervals == 0):
                # prints update at 'print_pages' intervals
                print('----- page {} -----'.format(i))
                print(page.page_id)
                time_delta = time.time() - t_start
                print('time elapse: {} --> time / page: {}'.format(time_delta, time_delta/(i+1)))

    time_delta = time.time() - t_start
    print('PROCESSED DATA: {} --> processing time / page: {}'.format(time_delta, time_delta/(i+1)))

    print('df.show():')
    print(df.show())
    print('df.schema:')
    df.printSchema()

    if write_output:
        print('WRITING TO FILE')
        # writes PySpark DataFrame to json file
        write_file_from_DataFrame(df=df, path=write_path)

    time_delta = time.time() - t_start
    print('JOB COMPLETE: {}'.format(time_delta))


def write_file_from_DataFrame(df, path, file_type='parquet'):
    """ Writes a PySpark DataFrame to different file formats """
    if file_type == 'parquet':
        df.write.parquet(path + '_' + str(time.time()))



if __name__ == '__main__':
    #read_path = '/nfs/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    read_path = '/nfs/trec_car/entity_processing/trec-car-entity-processing/data/test.pages.cbor'
    write_path = '/nfs/trec_car/data/test_entity/test.parquet'
    num_pages = 200
    print_intervals = 5
    write_output = True
    run_job(read_path=read_path, write_path=write_path, num_pages=num_pages, print_intervals=print_intervals,
            write_output=write_output)
