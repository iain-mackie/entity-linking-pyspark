
from utils.trec_car_tools import iter_pages, Para, ParaBody, ParaText, ParaLink, Section, Image, List



def get_doc(id, path):
    counter = 0
    with open(path, 'rb') as f:
        for p in iter_pages(f):
            doc = p

            if counter >= id:
                break
            counter += 1
    return doc


def print_doc_info(doc):

    print('==============================================================')
    print('page_name')
    print(doc.page_name)

    print('==============================================================')
    print('page_id')
    print(doc.page_id)

    print('==============================================================')
    print('page_type')
    # ArticlePage | CategoryPage | RedirectPage ParaLink | DisambiguationPage
    print(doc.page_type)

    print('==============================================================')
    print('get_text()')
    print(doc.get_text())

    print('==============================================================')
    print('outline')
    print('NOT REQUIRED')


def print_metadata(doc):

    print('==============================================================')
    print('page.meta\n')

    print('page_meta.redirectNames')
    print(doc.page_meta.redirectNames, '\n')

    print('page_meta.disambiguationNames')
    print(doc.page_meta.disambiguationNames, '\n')

    print('page_meta.disambiguationIds')
    print(doc.page_meta.disambiguationIds, '\n')

    print('page_meta.categoryNames')
    print(doc.page_meta.categoryNames, '\n')

    print('page_meta.categoryIds')
    print(doc.page_meta.categoryIds, '\n')

    print('page_meta.inlinkIds')
    print(doc.page_meta.inlinkIds, '\n')

    print('page_meta.inlinkAnchors')
    print(doc.page_meta.inlinkAnchors, '\n')


def print_bodies(b):
    for iB, B in enumerate(b):
        if isinstance(B, ParaLink):
            print('  ParaLink - paragraph.bodies()[{}].pageid:  |{}|'.format(iB, B.pageid))
            print('  ParaLink - paragraph.bodies()[{}].page:  |{}|'.format(iB, B.page))
            print('  ParaLink - paragraph.bodies()[{}].get_text():  |{}|'.format(iB, B.get_text()))
            print('  ParaLink - paragraph.bodies()[{}].link_section:  |{}|'.format(iB, B.link_section))
            print('  ParaLink - paragraph.bodies()[{}].anchor_text:  |{}|'.format(iB, B.anchor_text))
        elif isinstance(B, ParaText):
            print('  ParaText - paragraph.bodies()[{}].get_text(): |{}|'.format(iB, B.text))
        elif isinstance(B, ParaBody):
            print('  ParaBody - paragraph.bodies()[{}].get_text(): |{}|'.format(iB, B.get_text()))
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


def print_page_skeleton(ps):
    if isinstance(ps, Para):
        print_paragraph(p=ps)

    elif isinstance(ps, Image):
        print('IS IMAGE')
        print('  get_text():  {}'.format(ps.get_text()))
        print('  imageurl:  {}'.format(ps.imageurl))
        for cap in ps.caption:
            print('printing caption')
            print_paragraph(p=cap)

    elif isinstance(ps, Section):
        print_section(s=ps)

    elif isinstance(ps, List):
        print_list(l=ps)

    else:
        print("Page Section not type")
        raise


def print_skeleton(doc):

    print('page.skeleton\n')
    for i, ps in enumerate(doc.skeleton):
        print('==============================================================')
        print(' index: {}'.format(i))
        print_page_skeleton(ps)


def print_child_sections(doc):

    print('page.skeleton\n')
    for i, cs in enumerate(doc.child_sections):
        print('==============================================================')
        print(' index: {}'.format(i))
        print_page_skeleton(cs)


def print_head_and_child(head, children):
    print('  HEADING  ')
    print_section(s=head)
    if len(children) > 0:
        print('  CHILD HEADINGS  ')
        for h, c in children:
            print_head_and_child(head=h, children=c)
    else:
        print('  NO CHILDREN  ')


def print_nested_headings(doc):

    print('page.nested_headings()\n')
    for head, children in doc.nested_headings():
        print_head_and_child(head, children)
        print('==============================================================')

def print_flat_headings(doc):

    print('page.flat_headings()\n')
    for headings in doc.flat_headings_list():
        for h in headings:
            print_page_skeleton(ps=h)
            print('==============================================================')


def print_doc(doc):
    print('==============================================================')
    print('========================= NEW DOC ============================')
    print('==============================================================')

    # print_doc_info(doc)
    #
    # print_metadata(doc)

    print_skeleton(doc)
    #
    # print_child_sections(doc)
    #
    # print_nested_headings(doc)
    #
    # print_flat_headings(doc)



if __name__ == '__main__':

    #path = '/nfs/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    # path = '/Users/iain/LocalStorage/trec_page_data/unprocessedAllButBenchmark.Y2.cbor'
    path = '/Users/iain/LocalStorage/coding/github/trec-car-entity-processing/data/test.pages.cbor'
    #path = '/home/imackie/Documents/trec_car/data/pages/unprocessedAllButBenchmark.Y2.cbor'
    id = 3
    doc = get_doc(id=id, path=path)
    print_doc(doc=doc)

