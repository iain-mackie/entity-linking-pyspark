
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType

from data_exploration.data_exploration import get_doc
from trec_car_tools import iter_pages

import time


page_schema = StructType([
    StructField("page_id", StringType(), True),
    StructField("page_name", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("page_meta", MapType(StringType(), ArrayType(StringType(), True), True), True),
    StructField("text", StringType(), True),
])


def parse_inputs(page, spark, page_schema=page_schema):
    page_meta = {}
    page_meta['disambiguationNames'] = page.page_meta.disambiguationNames
    page_meta['disambiguationIds'] = page.page_meta.disambiguationIds
    page_meta['categoryNames'] = page.page_meta.disambiguationIds
    page_meta['categoryIds'] = page.page_meta.disambiguationIds
    page_meta['inlinkIds'] = page.page_meta.disambiguationIds
    page_meta['inlinkAnchors'] = page.page_meta.disambiguationIds
    return spark.createDataFrame([
                (page.page_id,
                 page.page_name,
                 str(page.page_type),
                 page_meta,
                 page.get_text())
            ], schema=page_schema)


def run_job(path, num_pages=1, print_pages=100):
    spark = SparkSession.builder.appName('trec_car').getOrCreate()
    t_start = time.time()
    with open(path, 'rb') as f:
        for i, page in enumerate(iter_pages(f)):

            TrecCarDataFrame = parse_inputs(page=page, spark=spark)

            if (i % print_pages == 0) and (i != 0):
                print('----- row {} -----'.format(i))
                print(TrecCarDataFrame.show())
                time_delta = time.time() - t_start
                print('time elapse: {} <> time / page: {}'.format(time_delta, time_delta/counter))


            if i >= num_pages:
                break



    t_end = time.time()
    time_delta = t_start - t_end
    print(time_delta)
    print(TrecCarDataFrame.show())
    print(TrecCarDataFrame.schema)


if __name__ == '__main__':
    path = '/nfs/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    num_pages = 10000
    run_job(path=path, num_pages=num_pages)
