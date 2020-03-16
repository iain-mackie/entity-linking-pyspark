

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
    StructField("skeleton", ArrayType(ArrayType(StringType(), True), True), True),
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


def parse_bodies(b):
    body_list = []
    for iB, B in enumerate(b):
        if isinstance(B, ParaLink):
            body_list.append(['ParaLink', B.pageid, B.page, B.get_text(), B.link_section])
        elif isinstance(B, ParaText):
            body_list.append(['ParaText', B.get_text()])
        elif isinstance(B, ParaBody):
            body_list.append(['ParaBody', B.get_text()])
        else:
            print("Paragraph body not type")
            raise
    return body_list


def parse_paragraph(skeleton_subclass, spacy_nlp):
    print('paragraph.get_text()')
    raw_byte_str = str(skeleton_subclass.paragraph.get_text().encode("utf-8"))
    raw_text = raw_byte_str[2:len(raw_byte_str)]
    print(raw_text)
    print(type(raw_text))

    raw_bytes = raw_text.encode("utf-8")
    print(raw_bytes)
    print(type(raw_bytes))

    doc = spacy_nlp(text=raw_text)
    print(list(doc.ents))
    return [skeleton_subclass.paragraph.para_id,
            skeleton_subclass.paragraph.get_text(),
            parse_bodies(b=skeleton_subclass.paragraph.bodies)]


def parse_skeleton_subclasses(skeleton_subclass, spacy_nlp):
    if isinstance(skeleton_subclass, Para):
        print('IS Para')
        return parse_paragraph(skeleton_subclass, spacy_nlp)

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


def parse_skeleton(skeleton, spacy_nlp):
    """ Parse page.skeleton to array """
    skeleton_list = []
    for i, skeleton_subclass in enumerate(skeleton):
        skeleton_list.append(parse_skeleton_subclasses(skeleton_subclass, spacy_nlp))
    return skeleton_list


def parse_metadata(page_meta):
    """ Parse page.page_data to dict """
    return {'disambiguationNames': page_meta.disambiguationNames,
            'disambiguationIds': page_meta.disambiguationIds,
            'categoryNames': page_meta.disambiguationIds,
            'categoryIds': page_meta.disambiguationIds,
            'inlinkIds': page_meta.disambiguationIds,
            'inlinkAnchors': page_meta.disambiguationIds}


def parse_page(page, i, spark, spacy_nlp, page_schema=page_schema):
    """ Builds a PySpark DataFrame given a Page and schema """
    return spark.createDataFrame([
                (i,
                 page.page_id,
                 page.page_name,
                 str(page.page_type),
                 parse_metadata(page.page_meta),
                 parse_skeleton(page.skeleton, spacy_nlp),
                )
            ], schema=page_schema)