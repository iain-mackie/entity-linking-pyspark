
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType

from data_exploration.data_exploration import get_doc
from trec_car_tools import iter_pages

import time
import json
import six


page_schema = StructType([
    StructField("idx", IntegerType(), True),
    StructField("page_id", StringType(), True),
    StructField("page_name", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("page_meta", MapType(StringType(), ArrayType(StringType(), True), True), True),
    StructField("text", StringType(), True),
])


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


def parse_inputs(page, i, spark, page_schema=page_schema):
    page_meta = {}
    page_meta['disambiguationNames'] = page.page_meta.disambiguationNames
    page_meta['disambiguationIds'] = page.page_meta.disambiguationIds
    page_meta['categoryNames'] = page.page_meta.disambiguationIds
    page_meta['categoryIds'] = page.page_meta.disambiguationIds
    page_meta['inlinkIds'] = page.page_meta.disambiguationIds
    page_meta['inlinkAnchors'] = page.page_meta.disambiguationIds
    return spark.createDataFrame([
                (i,
                 page.page_id,
                 page.page_name,
                 str(page.page_type),
                 page_meta,
                 page.get_text())
            ], schema=page_schema)


def write_json_from_DataFrame(df, path):

    with open(path, 'w') as f:
        for j in df.toJSON().collect():
            json.dump(j, f, indent=4)

def run_job(read_path, write_path, num_pages=1, print_pages=100):
    spark = SparkSession.builder.appName('trec_car').getOrCreate()
    t_start = time.time()
    with open(read_path, 'rb') as f:
        for i, page in enumerate(iter_pages(f)):

            if i == 0:
                df = parse_inputs(page=page, i=i, spark=spark)
            else:
                df = df.union(parse_inputs(page=page, i=i, spark=spark))

            if (i % print_pages == 0) and (i != 0):
                print('----- row {} -----'.format(i))
                print(page.page_id)
                time_delta = time.time() - t_start
                print('time elapse: {} --> time / page: {}'.format(time_delta, time_delta/i))

            if i >= num_pages:
                break

    time_delta = time.time() - t_start
    print('PROCESSED DATA: {}'.format(time_delta))

    print('WRITING TO JSON')
    write_json_from_DataFrame(df=df, path=write_path)
    time_delta = time.time() - t_start
    print('JOB COMPLETE: {}'.format(time_delta))



if __name__ == '__main__':
    read_path = '/nfs/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    write_path = '/nfs/trec_car/entity_processing/data/test.json'
    num_pages = 250
    run_job(read_path=read_path, write_path=write_path, num_pages=num_pages)
