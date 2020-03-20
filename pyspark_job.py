from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType, BinaryType
from pyspark.sql.functions import udf

from utils.trec_car_tools import iter_pages, Para, ParaBody, ParaText, ParaLink, Section, Image, List
from parse_trec_car import parse_page

import pickle
import spacy
import time
import json
import os
import json


def write_file_from_DataFrame(df, path, file_type='parquet'):
    """ Writes a PySpark DataFrame to different file formats """
    if file_type == 'parquet':
        df.write.parquet(path + '_' + str(time.time()))


# processing pyspark job
def get_pages_as_pickles(read_path, write_dir, num_pages=1, chunks=100000, print_intervals=100, write_output=False):
    """ Reads TREC CAR cbor file and returns list of Pages as bytearrays """
    # spacy_nlp = spacy.load("en_core_web_sm")

    if write_output:
        write_path = write_dir + 'data_' + str(time.time()) + '/'
        os.mkdir(write_path)

    data = []
    with open(read_path, 'rb') as f:
        t_start = time.time()
        for i, page in enumerate(iter_pages(f)):

            # stops when 'num_pages' processed
            if i >= num_pages:
                break

            # add
            data.append([bytearray(pickle.dumps(page))])

            if (i % chunks == 0) and (i != 0 or num_pages == 1):
                if write_output:
                    print('WRITING TO FILE')
                    # file_path = write_path + 'data' + str(time.time) + 'XXXX'
                    # write_to_file()
                    # data_list = []

            if (i % print_intervals == 0):
                # prints update at 'print_pages' intervals
                print('----- STEP {} -----'.format(i))
                time_delta = time.time() - t_start
                print('time elapse: {} --> time / page: {}'.format(time_delta, time_delta / (i + 1)))

    time_delta = time.time() - t_start
    print('PROCESSED DATA: {} --> processing time / page: {}'.format(time_delta, time_delta / (i + 1)))

    return data



def spark_processing(pages_as_pickles):

    spark = SparkSession.builder.appName('trec_car_spark').getOrCreate()

    # PySpark Schema
    schema = StructType([
        StructField("page_pickle", BinaryType(), True),
    ])

    df = spark.createDataFrame(data=pages_as_pickles, schema=schema)

    print('df.show():')
    print(df.show())
    print('df.schema:')
    df.printSchema()

    @udf(returnType=StringType())
    def page_id_udf(p):
        return pickle.loads(p).page_id

    @udf(returnType=StringType())
    def page_name_udf(p):
        return pickle.loads(p).page_name

    @udf(returnType=StringType())
    def page_type_udf(p):
        return str(pickle.loads(p).page_type)

    @udf(returnType=ArrayType(StringType()))
    def page_redirect_names_udf(p):
        return pickle.loads(p).page_meta.redirectNames

    @udf(returnType=ArrayType(StringType()))
    def page_disambiguation_names_udf(p):
        return pickle.loads(p).page_meta.disambiguationNames

    @udf(returnType=ArrayType(StringType()))
    def page_disambiguation_ids_udf(p):
        return pickle.loads(p).page_meta.disambiguationIds

    @udf(returnType=ArrayType(StringType()))
    def page_category_names_udf(p):
        return pickle.loads(p).page_meta.categoryNames

    @udf(returnType=ArrayType(StringType()))
    def page_category_ids_udf(p):
        return pickle.loads(p).page_meta.categoryIds

    @udf(returnType=ArrayType(StringType()))
    def page_inlink_ids_udf(p):
        return pickle.loads(p).page_meta.inlinkIds

    @udf(returnType=ArrayType(StructType([StructField("anchor_text", StringType()),StructField("frequency", IntegerType())])))
    def page_inlink_anchors_udf(p):
        return pickle.loads(p).page_meta.inlinkAnchors

    @udf(returnType=BinaryType())
    def page_skeleton_pickle_udf(p):
        return bytearray(pickle.dumps(pickle.loads(p).skeleton))

    # @udf(returnType=BinaryType())
    # def synthetic_page_skeleton_pickle_udf(s):
    #     skeleton_list = []
    #     skeleton = pickle.loads(s)
    #     for i, skeleton_subclass in enumerate(skeleton):
    #         if isinstance(skeleton_subclass, Para):
    #             print('IS Para')
    #             text = skeleton_subclass.paragraph.get_text()
    #
    #
    #         # elif isinstance(skeleton_subclass, Image):
    #         #     print('IS IMAGE')
    #         #     return skeleton_subclass
    #         #
    #         # elif isinstance(skeleton_subclass, Section):
    #         #     print('IS Section')
    #         #     return skeleton_subclass
    #         #
    #         # elif isinstance(skeleton_subclass, List):
    #         #     print('IS List')
    #         #     return skeleton_subclass
    #         #
    #         # else:
    #         #     print("Page Section not type")
    #         #     raise
    #         # skeleton_list.append(skeleton_subclass)
    #     return bytearray(pickle.dumps(skeleton_list))


    # @udf(returnType=ArrayType(StringType()))
    # def synthetic_paragraphs_udf(s):
    #     paragraph_list = []
    #     skeleton = pickle.loads(s)
    #     for i, skeleton_subclass in enumerate(skeleton):
    #         if skeleton_subclass == Para:
    #             paragraph_list.append(skeleton_subclass)
    #     return bytearray(pickle.dumps(paragraph_list))

    # sythetics_inlink_anchors

    # sythetics_inlink_ids


    df = df.withColumn("page_id", page_id_udf("page_pickle"))
    df = df.withColumn("page_name", page_name_udf("page_pickle"))
    df = df.withColumn("page_type", page_type_udf("page_pickle"))
    df = df.withColumn("redirect_names", page_redirect_names_udf("page_pickle"))
    df = df.withColumn("disambiguation_names", page_disambiguation_names_udf("page_pickle"))
    df = df.withColumn("disambiguation_ids", page_disambiguation_ids_udf("page_pickle"))
    df = df.withColumn("category_names", page_category_names_udf("page_pickle"))
    df = df.withColumn("category_ids", page_category_ids_udf("page_pickle"))
    df = df.withColumn("inlink_ids", page_inlink_ids_udf("page_pickle"))
    df = df.withColumn("inlink_anchors", page_inlink_anchors_udf("page_pickle"))
    df = df.withColumn("skeleton", page_skeleton_pickle_udf("page_pickle"))
    # df = df.withColumn("synthetic_skeleton", synthetic_page_skeleton_pickle_udf("skeleton"))
    # df = df.withColumn("synthetic_paragraphs", synthetic_paragraphs_udf("synthetic_skeleton"))

    print('df.show():')
    print(df.show())
    print('df.schema:')
    df.printSchema()

    return df


def run_spark_job(read_path, write_dir, num_pages=1, chunks=100000, print_intervals=100, write_output=False):

    pages_as_pickles = get_pages_as_pickles(read_path=read_path,
                                            write_dir=write_dir,
                                            num_pages=num_pages,
                                            chunks=chunks,
                                            print_intervals=print_intervals,
                                            write_output=write_output)

    return spark_processing(pages_as_pickles=pages_as_pickles)


if __name__ == '__main__':
    #read_path = '/nfs/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    read_path = '/nfs/trec_car/entity_processing/trec-car-entity-processing/data/test.pages.cbor'
    write_dir = '/nfs/trec_car/data/test_entity/'
    num_pages = 200
    print_intervals = 10
    write_output = False
    chunks = 10
    df = run_spark_job(read_path=read_path,  write_dir=write_dir, num_pages=num_pages, chunks=chunks,
                       print_intervals=print_intervals, write_output=write_output)
