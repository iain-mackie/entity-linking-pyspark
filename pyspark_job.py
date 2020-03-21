from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType, BinaryType
from pyspark.sql.functions import udf

from utils.trec_car_tools import iter_pages, Para, Paragraph, ParaBody, ParaText, ParaLink, Section, Image, List

import pickle
import spacy
import time
import json
import os
import json


# define valid classes
SKELETON_CLASSES = (Para, List, Section, Image)
PARAGRAPH_CLASSES = (Paragraph)


def write_file_from_DataFrame(df, path, file_type='parquet'):
    """ Writes a PySpark DataFrame to different file formats """
    if file_type == 'parquet':
        df.write.parquet(path + '_' + str(time.time()))


def get_pages_data(read_path, write_dir, num_pages=1, chunks=100000, print_intervals=100, write_output=False):
    """ Reads TREC CAR cbor file and returns list of Pages as bytearrays """

    # create new dir to store data chunks
    if write_output:
        write_path = write_dir + 'data_' + str(time.time()) + '/'
        os.mkdir(write_path)

    pages_data = []
    with open(read_path, 'rb') as f:
        t_start = time.time()
        for i, page in enumerate(iter_pages(f)):

            # stops when 'num_pages' processed
            if i >= num_pages:
                break

            # add bytearray of trec_car_tool.Page object
            pages_data.append([bytearray(pickle.dumps(page))])

            # write data chunk to file
            if (i % chunks == 0) and (i != 0 or num_pages == 1):
                if write_output:
                    print('WRITING TO FILE')
                    # file_path = write_path + 'data' + str(time.time) + 'XXXX'
                    # write_to_file()
                    # data_list = []

            # prints update at 'print_pages' intervals
            if (i % print_intervals == 0):
                print('----- STEP {} -----'.format(i))
                time_delta = time.time() - t_start
                print('time elapse: {} --> time / page: {}'.format(time_delta, time_delta / (i + 1)))

    time_delta = time.time() - t_start
    print('PROCESSED DATA: {} --> processing time / page: {}'.format(time_delta, time_delta / (i + 1)))

    return pages_data


def pyspark_processing(pages_data):
    """ PySpark pipeline for adding syethetic entity linking and associated metadata """

    spark = SparkSession.builder.appName('trec_car_spark').getOrCreate()

    # PySpark Schema
    schema = StructType([
        StructField("page_pickle", BinaryType(), True),
    ])

    # creare pyspark DataFrame where each row in a bytearray of trec_car_tool.Page object
    df = spark.createDataFrame(data=pages_data, schema=schema)

    #TODO - remove in production
    print('df.show():')
    print(df.show())
    print('df.schema:')
    df.printSchema()

    #TODO - multiple columns
    #TODO - do we need al these features explosed in production?
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
    def page_skeleton_udf(p):
        return bytearray(pickle.dumps(pickle.loads(p).skeleton))

    @udf(returnType=StructType([StructField("skeleton", BinaryType()),StructField("paragraphs", BinaryType())]))
    def synthetic_page_skeleton_and_paragraphs_udf(p):
        """ PySpark udf creating a new Page.skeleton with synthetic entity linking + paragraph list """
        #TODO - multiple columns

        def get_bodies_from_text(spacy_model, text):
            """ build list of trec_car_tools ParaText & ParaLink objects (i.e. bodies) from raw text """
            # nlp process text
            doc = spacy_model(text=text)
            # extract NED (named entity detection) features
            ned_data = [(ent.text, ent.start_char, ent.end_char) for ent in doc.ents]

            text_i = 0
            text_end = len(text)
            new_text = ''
            bodies = []
            for span, start_i, end_i in ned_data:
                if text_i < start_i:
                    # add ParaText object to bodies list
                    current_span = text[text_i:start_i]
                    bodies.append(ParaText(text=current_span))
                    new_text += current_span

                # add ParaLink object to bodies list
                current_span = span
                new_text += current_span
                #TODO - entity linking
                bodies.append(ParaLink(page='STUB_PAGE',
                                       pageid='STUB_PAGE_ID',
                                       link_section=None,
                                       anchor_text=current_span))
                text_i = end_i

            if text_i < text_end:
                # add ParaText object to bodies list
                current_span = text[text_i:text_end]
                bodies.append(ParaText(text=current_span))
                new_text += current_span

            # assert appended current_span equal original text
            assert new_text == text, {"TEXT: {} \nNEW TEXT: {}"}

            return bodies


        def parse_skeleton_subclass(skeleton_subclass, spacy_model):
            """ parse PageSkeleton object {Para, Image, Section, Section} with new entity linking """

            if isinstance(skeleton_subclass, Para):
                para_id = skeleton_subclass.paragraph.para_id
                text = skeleton_subclass.paragraph.get_text()
                # add synthetic entity linking
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
                    # loop over Section.children to add entity linking and re-configure to original dimensions
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
                # add synthetic entity linking
                bodies = get_bodies_from_text(spacy_model=spacy_model, text=text)
                #TODO - what is a paragraph??
                p = Paragraph(para_id=para_id, bodies=bodies)
                return List(level=level, body=p), p

            else:
                raise ValueError("Not expected class")

        def parse_skeleton(skeleton, spacy_model):
            """ parse Page.skeleton (i.e. list of PageSkeleton objects) and add synthetic entity linking """

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

        # initialise spacy_model
        spacy_model = spacy.load("en_core_web_sm")
        # extract skeleton (list of PageSkeleton objects)
        skeleton = pickle.loads(p).skeleton

        synthetic_skeleton, synthetic_paragraphs = parse_skeleton(skeleton=skeleton, spacy_model=spacy_model)

        return bytearray(pickle.dumps(synthetic_skeleton)), bytearray(pickle.dumps(synthetic_paragraphs))

    #TODO -  sythetics_inlink_anchors

    #TODO - sythetics_inlink_ids

    # add PySpark rows
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
    df = df.withColumn("skeleton", page_skeleton_udf("page_pickle"))
    # df = df.withColumn("synthetic_entity_linking", synthetic_page_skeleton_and_paragraphs_udf("page_pickle"))

    # TODO - remove in production
    print('df.show():')
    print(df.show())
    print('df.schema:')
    df.printSchema()

    return df


def run_pyspark_job(read_path, write_dir, num_pages=1, chunks=100000, print_intervals=100, write_output=False):

    # extract page data from
    pages_data = get_pages_data(read_path=read_path,
                                write_dir=write_dir,
                                num_pages=num_pages,
                                chunks=chunks,
                                print_intervals=print_intervals,
                                write_output=write_output)

    # process page data adding synthetic entity links
    return pyspark_processing(pages_data=pages_data)


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
