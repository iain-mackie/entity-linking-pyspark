

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType

from trec_car_tools import Para, ParaBody, ParaText, ParaLink, Section, Image, List

import spacy
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


def print_bodies(b):
    for iB, B in enumerate(b):
        if isinstance(B, ParaLink):
            print('  ParaLink - paragraph.bodies()[{}].pageid:  {}'.format(iB, B.get_text()))
            print('  ParaLink - paragraph.bodies()[{}].page:  {}'.format(iB, B.page))
            print('  ParaLink - paragraph.bodies()[{}].get_text():  {}'.format(iB, B.get_text()))
            print('  ParaLink - paragraph.bodies()[{}].link_section:  {}'.format(iB, B.link_section))
        elif isinstance(B, ParaText):
            print('  ParaText - paragraph.bodies()[{}].get_text(): {}'.format(iB, B.get_text()))
        elif isinstance(B, ParaBody):
            print('  ParaBody - paragraph.bodies()[{}].get_text(): {}'.format(iB, B.get_text()))
        else:
            print("Paragraph not type")
            raise


def print_paragraph(p):
    print('\nPara')
    print('  paragraph.para_id : {}'.format(p.paragraph.para_id))
    print('  paragraph.get_text() : {}'.format(p.paragraph.get_text()))
    print_bodies(b=p.paragraph.bodies)


def print_list(l):
    print('\nList')
    print('  level: {}'.format(l.level))
    print('  get_text(): {}'.format(l.get_text()))
    print('  body.para_id: {}'.format(l.body.para_id))
    print('  body.get_text(): {}'.format(l.body.get_text()))
    print_bodies(b=l.body.bodies)


def print_section(s):
    print('\nSection')
    print('  heading: {}'.format(s.heading))
    print('  headingId: {}'.format(s.headingId))
    print('  nested_headings: {}'.format(s.nested_headings()))
    child_sections = s.child_sections
    if len(child_sections) == 0:
        print('  empty child_sections: {}'.format(s.child_sections))
    else:
        for CS in child_sections:
            print('*** printing child_sections ***')
            print_section(s=CS)

    children = s.children
    if len(children) == 0:
        print('  empty children: {}'.format(children))
    else:
        for C in children:
            print('*** printing children ***')
            print_page_skeleton(ps=C)

def parse_skeleton_subclasses(skeleton_subclass):
    if isinstance(skeleton_subclass, Para):
        print('IS Para')
        # print(skeleton_subclass)

    elif isinstance(skeleton_subclass, Image):
        print('IS IMAGE')
        # print(skeleton_subclass)

    elif isinstance(skeleton_subclass, Section):
        print('IS Section')
        # print(skeleton_subclass)

    elif isinstance(skeleton_subclass, List):
        print('IS List')
        # print(skeleton_subclass)

    else:
        print("Page Section not type")
        raise

def parse_skeleton(skeleton):
    """ Parse page.skeleton to array """
    for i, skeleton_subclass in enumerate(skeleton):
        parse_skeleton_subclasses(skeleton_subclass)
    return ['STRING', 'STRING']


def parse_metadata(page_meta):
    """ Parse page.page_data to dict """
    d = {}
    d['disambiguationNames'] = page_meta.disambiguationNames
    d['disambiguationIds'] = page_meta.disambiguationIds
    d['categoryNames'] = page_meta.disambiguationIds
    d['categoryIds'] = page_meta.disambiguationIds
    d['inlinkIds'] = page_meta.disambiguationIds
    d['inlinkAnchors'] = page_meta.disambiguationIds
    return d


def parse_page(page, i, spark, spacy_nlp, page_schema=page_schema):
    """ Builds a PySpark DataFrame given a Page and schema """
    parse_skeleton(page.skeleton)
    return spark.createDataFrame([
                (i,
                 page.page_id,
                 page.page_name,
                 str(page.page_type),
                 parse_metadata(page.page_meta),
                 parse_skeleton(page.skeleton),
                )
            ], schema=page_schema)