
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


def run_job(path, num_pages=1):
    spark = SparkSession.builder.appName('trec_car').getOrCreate()
    counter = 0
    t0 = time.time()
    with open(path, 'rb') as f:
        for page in iter_pages(f):
            TrecCarDataFrame = parse_inputs(page=page, spark=spark)
            if counter >= num_pages:
                break
            counter += 1

    t1 = time.time()
    time_delta = t1 - t0
    print(time_delta)
    print(TrecCarDataFrame.show())
    for i in TrecCarDataFrame.schema:
        print(i)


if __name__ == '__main__':
    path = '/nfs/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    num_pages = 100
    run_job(path=path, num_pages=num_pages)
