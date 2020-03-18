

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, IntegerType

from utils.trec_car_tools import Para, ParaBody, ParaText, ParaLink, Section, Image, List

import spacy
import six

# PySpark Schema
page_schema = StructType([
    StructField("idx", IntegerType(), True),
    StructField("page_id", StringType(), True),
    StructField("page_name", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("redirectNames", ArrayType(StringType(), True), True),
    StructField("disambiguationNames", ArrayType(StringType(), True), True),
    StructField("disambiguationIds", ArrayType(StringType(), True), True),
    StructField("categoryNames", ArrayType(StringType(), True), True),
    StructField("categoryIds", ArrayType(StringType(), True), True),
    StructField("inlinkIds", ArrayType(StringType(), True), True),
    StructField("inlinkAnchors", ArrayType(
        StructType([
            StructField("class", StringType(), False),
            StructField("description", IntegerType(), False)]),
        True))
])
page_names = ["idx", "page_id", "page_name", "page_type", "page_meta", "skeleton"]

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

def fix_encoding(s):
    # to stop UnicodeEncodeError: 'ascii' codec can't encode character error
    raw_byte_str = str(s.encode("utf-8"))
    raw_text = raw_byte_str[2:len(raw_byte_str)-1]
    return raw_text

def parse_bodies(b):
    body_list = []
    for iB, B in enumerate(b):
        if isinstance(B, ParaLink):
            body_list.append(['ParaLink', B.pageid, B.page, fix_encoding(s=B.get_text()), B.link_section])
        elif isinstance(B, ParaText):
            body_list.append(['ParaText', fix_encoding(s=B.get_text())])
        elif isinstance(B, ParaBody):
            body_list.append(['ParaBody', fix_encoding(s=B.get_text())])
        else:
            print("Paragraph body not type")
            raise
    return body_list


def parse_paragraph(skeleton_subclass, spacy_nlp, write_para=True):
    print('paragraph.get_text()')

    raw_text = fix_encoding(s=skeleton_subclass.paragraph.get_text())
    print(raw_text)

    doc = spacy_nlp(text=raw_text)
    print(list(doc.ents))

    print(parse_bodies(b=skeleton_subclass.paragraph.bodies))

    parse_paragraph =  [skeleton_subclass.paragraph.para_id,
                        skeleton_subclass.paragraph.get_text(),
                        parse_bodies(b=skeleton_subclass.paragraph.bodies)]

    if write_para:
        print('*** write para ***')

    return parse_paragraph


def parse_skeleton_subclasses(skeleton_subclass, spacy_nlp, write_para=False):
    if isinstance(skeleton_subclass, Para):
        print('IS Para')
        return parse_paragraph(skeleton_subclass, spacy_nlp, write_para)

    elif isinstance(skeleton_subclass, Image):
        print('IS IMAGE')
        return [['IMAGE']]

    elif isinstance(skeleton_subclass, Section):
        print('IS Section')
        return [['Section']]

    elif isinstance(skeleton_subclass, List):
        print('IS List')
        return [['List']]

    else:
        print("Page Section not type")
        raise


def parse_skeleton(skeleton, spacy_nlp, write_para=False):
    """ Parse page.skeleton to array """
    skeleton_list = []
    for i, skeleton_subclass in enumerate(skeleton):
        skeleton_list.append(parse_skeleton_subclasses(skeleton_subclass=skeleton_subclass,
                                                       spacy_nlp=spacy_nlp,
                                                       write_para=write_para))
    return skeleton_list


def parse_metadata(page_meta):
    """ Parse page.page_data to dict """
    return {'redirectNames': page_meta.redirectNames,
            'disambiguationNames': page_meta.disambiguationNames,
            'disambiguationIds': page_meta.disambiguationIds,
            'categoryNames': page_meta.disambiguationIds,
            'categoryIds': page_meta.disambiguationIds,
            'inlinkIds': page_meta.disambiguationIds,
            'inlinkAnchors': page_meta.disambiguationIds}


def parse_page(page, i, spark, spacy_nlp, page_schema=page_schema, write_para=False):
    """ Builds a PySpark DataFrame given a Page and schema """
    return spark.createDataFrame([
                (i,
                 page.page_id,
                 page.page_name,
                 str(page.page_type),
                 page.page_meta.redirectNames,
                 page.page_meta.disambiguationNames,
                 page.page_meta.disambiguationIds,
                 page.page_meta.categoryNames,
                 page.page_meta.categoryIds,
                 page.page_meta.inlinkIds,
                 page.page_meta.inlinkAnchors,
                 # parse_skeleton(skeleton=page.skeleton, spacy_nlp=spacy_nlp, write_para=True),
                )
            ], schema=page_schema)


