
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType

from data_exploration.data_exploration import get_doc
from trec_car_tools import iter_pages

import spacy
import time
import json
import six

# PySpark Schema
page_schema = StructType([
    StructField("idx", IntegerType(), True),
    StructField("page_id", StringType(), True),
    StructField("page_name", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("page_meta", MapType(StringType(), ArrayType(StringType(), True), True), True),
    StructField("skeleton", ArrayType(StringType(), True), True),
])


def write_json_from_DataFrame(df, path):
    """ Writes a PySpark DataFrame to json file """
    with open(path, 'a+') as f:
        for j in df.toJSON().collect():
            json.dump(j, f, indent=4)


def convert_to_unicode(text):
    """Converts `text` to Unicode (if it's not already), assuming utf-8 input."""
    if six.PY3:
        if isinstance(text, str):
            return text
        elif isinstance(text, bytes):
            return text.decode("utf-8", "ignore")
        else:
            raise ValueError("Unsupported string type: %s" % (type(text)))
    else:
        raise ValueError("Not running on Python 3?")


def parse_skeleton(skeleton):
    print(skeleton)
    return ['STRING', 'STRING']


def parse_metadata(page_meta):
    d = {}
    d['disambiguationNames'] = page_meta.disambiguationNames
    d['disambiguationIds'] = page_meta.disambiguationIds
    d['categoryNames'] = page_meta.disambiguationIds
    d['categoryIds'] = page_meta.disambiguationIds
    d['inlinkIds'] = page_meta.disambiguationIds
    d['inlinkAnchors'] = page_meta.disambiguationIds
    return d


def parse_inputs(page, i, spark, spacy_nlp, page_schema=page_schema):
    """ Builds a PySpark DataFrame given a Page and schema """
    parse_skeleton(page.skeleton)
    return spark.createDataFrame([
                (i,
                 page.page_id,
                 page.page_name,
                 str(page.page_type),
                 parse_metadata(page.page_meta),
                )
            ], schema=page_schema)


def run_job(read_path, write_path, num_pages=1, print_intervals=100):
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
            df = parse_inputs(page=page, i=i, spark=spark, spacy_nlp=spacy_nlp)

            # writes PySpark DataFrame to json file
            write_json_from_DataFrame(df=df, path=write_path)

            if (i % print_intervals == 0):
                # prints update at 'print_pages' intervals
                print('----- page {} -----'.format(i))
                print(page.page_id)
                time_delta = time.time() - t_start
                print('time elapse: {} --> time / page: {}'.format(time_delta, time_delta/(i+1)))

    time_delta = time.time() - t_start
    print('PROCESSED DATA: {} --> processing time / page: {}'.format(time_delta, time_delta/(i+1)))


if __name__ == '__main__':
    read_path =  '/nfs/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    write_path = '/nfs/trec_car/data/test_entity/test.json'
    num_pages = 10
    print_intervals = 1
    run_job(read_path=read_path, write_path=write_path, num_pages=num_pages, print_intervals=print_intervals)
