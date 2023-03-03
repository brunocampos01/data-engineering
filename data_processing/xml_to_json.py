import json
import sys
import xml.etree.ElementTree as ET
import os

here = os.path.abspath(os.path.dirname(__file__))
path_origin = ''.join(here + '/fruits.xml')
path_dest = here + '/fruits.csv'


def load_xml(path_json: str) -> str:
    return open(path_json, 'r').read()


def xml_to_dict(element) -> dict:
    # Base case: element has no children
    if not element:
        return element.text

    # Recursive case: element has children
    d = {}
    for child in element:
        if child.tag not in d:
            d[child.tag] = []
        d[child.tag].append(xml_to_dict(child))

    # Add element attributes, if any
    if element.attrib:
        d.update(element.attrib)

    return d


def encoding_json(xml_d: str) -> None:
    root = ET.fromstring(xml_d)
    with open(f'fruits.json', 'w') as writer:
        json.dump(xml_to_dict(root), writer)


if __name__ == '__main__':
    xml_data = load_xml(path_origin)
    encoding_json(xml_data)
