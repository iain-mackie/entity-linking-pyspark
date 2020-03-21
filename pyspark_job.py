from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType, BinaryType
from pyspark.sql.functions import udf

from utils.trec_car_tools import iter_pages, Para, Paragraph, ParaBody, ParaText, ParaLink, Section, Image, List
from parse_trec_car import parse_page

import pickle
import spacy
import time
import json
import os
import json

SKELETON_CLASSES = (Para, List, Section, Image)
PARAGRAPH_CLASSES = (Paragraph)

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



def pyspark_processing(pages_as_pickles):

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

    # TODO - multiple columns
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

    @udf(returnType=BinaryType())
    def synthetic_page_skeleton_pickle_udf(p):
        #TODO - multiple columns

        def get_bodies_from_text(spacy_model, text):
            doc = spacy_model(text)
            ned_data = [(ent.text, ent.start_char, ent.end_char) for ent in doc.ents]

            text_i = 0
            text_end = len(text)
            new_text = ''
            bodies = []
            for span, start_i, end_i in ned_data:
                if text_i < start_i:
                    current_span = text[text_i:start_i]
                    bodies.append(ParaText(text=current_span))
                    new_text += current_span

                current_span = span
                new_text += current_span
                bodies.append(ParaLink(page='BLANK',
                                       pageid='BLANK',
                                       link_section=None,
                                       anchor_text=current_span))
                text_i = end_i

            if text_i < text_end:
                current_span = text[text_i:text_end]
                bodies.append(ParaText(text=current_span))
                new_text += current_span

            assert new_text == text, {"TEXT: {} \nNEW TEXT: {}"}

            return bodies

        def parse_skeleton_subclass(skeleton_subclass, spacy_model):

            if isinstance(skeleton_subclass, Para):
                para_id = skeleton_subclass.paragraph.para_id
                text = skeleton_subclass.paragraph.get_text()
                bodies = get_bodies_from_text(spacy_model=spacy_model, text=text)
                p = Paragraph(para_id=para_id, bodies=bodies)
                return Para(p), p

            elif isinstance(skeleton_subclass, Image):
                caption = skeleton_subclass.caption
                # TODO - what is a paragraph??
                s, p = parse_skeleton_subclass(skeleton_subclass=caption, spacy_model=spacy_model)
                imageurl = skeleton_subclass.imageurl
                return Image(imageurl=imageurl, caption=s), p

            elif isinstance(skeleton_subclass, Section):
                heading = skeleton_subclass.heading
                headingId = skeleton_subclass.headingId
                children = skeleton_subclass.children

                if len(children) == 0:
                    return Section(heading=heading, headingId=headingId, children=children), []

                else:
                    s_list = []
                    p_list = []
                    for c in children:
                        s, p = parse_skeleton_subclass(skeleton_subclass=c, spacy_model=spacy_model)
                        if isinstance(s, SKELETON_CLASSES):
                            s_list.append(s)
                        if isinstance(p, list):
                            for paragraph in p:
                                if isinstance(paragraph, PARAGRAPH_CLASSES):
                                    p_list.append(paragraph)
                        else:
                            if isinstance(p, PARAGRAPH_CLASSES):
                                p_list.append(p)
                    return Section(heading=heading, headingId=headingId, children=s_list), p_list

            elif isinstance(skeleton_subclass, List):
                level = skeleton_subclass.level
                para_id = skeleton_subclass.body.para_id
                text = skeleton_subclass.get_text()
                bodies = get_bodies_from_text(spacy_model=spacy_model, text=text)
                #TODO - what is a paragraph??
                #TODO - Para or Paragraph as body?
                p = Paragraph(para_id=para_id, bodies=bodies)
                return List(level=level, body=p), p

            else:
                print("Not expected class")
                raise

        def parse_skeleton(skeleton, spacy_model):

            synthetic_skeleton = []
            synthetic_paragraphs = []
            for i, skeleton_subclass in enumerate(skeleton):
                s, p = parse_skeleton_subclass(skeleton_subclass, spacy_model)
                if isinstance(s, SKELETON_CLASSES):
                    synthetic_skeleton.append(s)
                if isinstance(p, list):
                    for paragraph in p:
                        if isinstance(paragraph, PARAGRAPH_CLASSES):
                            synthetic_paragraphs.append(paragraph)
                else:
                    if isinstance(p, PARAGRAPH_CLASSES):
                        synthetic_paragraphs.append(p)

            return synthetic_skeleton, synthetic_paragraphs

        spacy_model = spacy.load("en_core_web_sm")
        skeleton = pickle.loads(p).skeleton
        synthetic_skeleton, synthetic_paragraphs = parse_skeleton(skeleton, spacy_model)

        return bytearray(pickle.dumps((synthetic_skeleton, synthetic_paragraphs)))

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
    df = df.withColumn("synthetic_skeleton_and_paragraphs", synthetic_page_skeleton_pickle_udf("page_pickle"))
    # df = df.withColumn("synthetic_paragraphs", synthetic_paragraphs_udf("synthetic_skeleton"))

    print('df.show():')
    print(df.show())
    print('df.schema:')
    df.printSchema()

    return df


def run_pyspark_job(read_path, write_dir, num_pages=1, chunks=100000, print_intervals=100, write_output=False):

    pages_as_pickles = get_pages_as_pickles(read_path=read_path,
                                            write_dir=write_dir,
                                            num_pages=num_pages,
                                            chunks=chunks,
                                            print_intervals=print_intervals,
                                            write_output=write_output)

    return pyspark_processing(pages_as_pickles=pages_as_pickles)


if __name__ == '__main__':
    #read_path = '/nfs/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    read_path = '/nfs/trec_car/entity_processing/trec-car-entity-processing/data/test.pages.cbor'
    write_dir = '/nfs/trec_car/data/test_entity/'
    num_pages = 200
    print_intervals = 10
    write_output = False
    chunks = 10
    df = run_pyspark_job(read_path=read_path,  write_dir=write_dir, num_pages=num_pages, chunks=chunks,
                         print_intervals=print_intervals, write_output=write_output)
