'''
pyarrow._docutils

This module processes the reStructured text additions located in `docs/additions` so that they can be incorporated 
into the python function definitions. The possible xml generated from rst files is nondeterministic, and only a 
small and highly structured syntax is supported. See the files ins `docs/additions` to review what is allowed 
and make updates to this module to process those expectations.

'''

import docutils
import docutils.core
import docutils.io
import re
import os
import pdb 


def parse_directory(path:str):
    """
    Parse all reStructured text files within a directory and process them as function additions.

    Args:
        path: The path to the directory.
    Returns:
        dict: a map of the filename to the documentation details as provided by `parse_function_rst`.    
    """
    results = {}
    files = os.listdir(path)
    rstre = re.compile(r'\.rst$', re.I)
    for file in files:
        if not rstre.search(file):
            continue
        function_data = parse_rst(path + os.sep + file)
        results.update(function_data)
    return(results)

def parse_rst(filename:str) -> dict:
    """ 
    Parse a reStructured Text file to provide function documentation details.

    Args:
        filename: The path to the file to be processed
    Returns:
        dict: A data structure that is keyed based on the function name and provides override documentation details as follows:
            'function': The lowercase function name
            'text_segments': a list of 2-element tuples of the subsection heading and the subsection text. These are often notes are additional informative blocks of text that should follow the main description.
            'parameters': A dict keyed by the parameter name that maps to a subsequent dict with the following strucutre:
                'definition': If provided, an override block to include as the definition for the parameter. If multiple paragraphs in the source rST, they will be coded as separated by 2 newlines.
                'classifiers': If provided, an override to the "type" argument for the parameter. If multiple types are provided, they will be joined by a pipe.
            'examples': The raw code to provide for examples. Note that the original indent will be stripped.
            'returns': A list of 2-element tuples of (data type, description)
    """

    doc = docutils.core.publish_doctree(
        source=docutils.io.FileInput(source_path = filename),
        source_class=docutils.io.FileInput
    )

    functiondata = {}
    # find the function name, which is the title
    i = 0
    function_name = None
    while i < len(doc.children):
        if doc.children[i].tagname != 'title':
            i += 1
            continue
        function_name = doc.children[i].rawsource.strip()
        break
    if not function_name:
        raise Exception(f"rst file is not formatted as as expected for a pyarrow function doc addtion: {filename}")
    print(f"-- function_name is {function_name} ({filename})", flush= True)
    functiondata[function_name] = parse_function_rst(doc.children[i+1:])
    return(functiondata)

def _process_definition_list(node):
    """
    Process a reStructured Text definition node.

    Given a node that is a `definition_list`, return a usalbe data structure of the information contained within.

    Args:
        node: The node to process
    Returns:
        list(dict): A list of the dictionaries of the following keys:
            'term': The name of the definition term.
            'classifiers': A list of classifiers. It may be an empty list if no classifiers existed in the source reStructured Text.
            'definition': A text string denoting the definition. In the case that the definition has multiple paragraphs, the paragraphs will be separated by two newlines. 

    """
    results = []
    if node.tagname != "definition_list":
        raise Exception(f"reStructured text 'returns' override for pyarrow functions should be contained in a single definition list (line {node.line} in the .rst file))")
    for term_node, *details in node.children:
        if term_node.tagname != 'term':
            raise Exception(f"reStructured text parameter override must start with a term (line {term_node.line} in the .rst file))")
        term = term_node.astext()

        classifiers = []
        while details and details[0].tagname == "classifier":
            classifiers.append(details[0].astext())
            details.pop(0)

        definitions = []
        while details and details[0].tagname == "definition":
            definitions.append(details[0].astext())
            details.pop(0)
        
        if details:
            raise Exception(f"unknown dictionary parameter override value '{details[0].tagname}'. Expecting on classifiers and definitions (line {details[0].line} in the .rst file))")
        results.append({'term': term, 'classifiers': classifiers, 'definition': "\n\n".join(definitions)})

    return(results)

def parse_function_rst(nodes):
    """ 
    Parse the reStructured text for a specific function.

    Note:
        Due to the complexity of the reStructured text specification, the parts may be identified as either a subtitle or a section, depending on whether there's a single subheading or multiple. Thus, we need to handle both. 

        It may also be valuable to note that reSt does not allow for two sections of the same name in the same file, which is why the additions documentions must each be separated into separate files.
    
    Args:
        node: A docutils section node. All details are children within the section.
    Returns:
        dict: a map of the documentation details of the reStructured text node to the values provided. Currently supports:
            'function': The function name as defined in the reSt document.
            'description': The full description for the function.
            'details': Intended to be additional description documentation, i.e. paragraphs that follow the main description.
            'parameters': A dict of each parameter name to:
                'type': A pipe separated string of possible type values derived from the reSt definitoin.
                'definition': The text defining the parameter.
            'returns': A list of 2-element tuples of (return type, description of what it means)
            'examples': A list of examples text elements.
    Raises:
        Exception if a section does not conform to the above definition.
    """
    result = {}
    section_name = None
    for node in nodes:
        if node.tagname == "note" or node.tagname == "comment":
            # this handles note and comment tags, which we'll interpret as comments to the arrow developers regarding the doc additions, and not as part of the actual end-user documentation.        
            continue
        if node.tagname == 'subtitle':
            # This happens if there's only one section in the file
            section_name = node[0].astext().strip().lower()
            if section_name == 'comment':
                # same comment as above - if there's a section called comment, consider it a section for the developers and not the end-users.
                continue
            if section_name not in ('description', 'details', 'examples', 'note', 'notes', 'parameters', 'returns'):
                raise Exception(f"Unknown section '{section_name}' found in function rst file(from line {node.line} in the .rst file)")
            if section_name in ('parameters'):
                result.setdefault(section_name, {})
            else:
                result.setdefault(section_name, [])
            if len(node.children) == 1:
                continue
            elif len(node.children) == 2:
                node = node.children[1]
            else:
                raise Exception('Unexpected number of children for subtitle')
        elif node.tagname == 'section':
            section_name = node[0].astext().strip().lower()
            if section_name == 'comment':
                # same comment as above - if there's a section called comment, consider it a section for the developers and not the end-users.
                continue
            if section_name not in ('description', 'details', 'examples', 'note', 'notes', 'comment', 'parameters', 'returns'):
                raise Exception(f"Unknown section '{section_name}' found in function rst file(from line {node.line} in the .rst file)")
            if section_name in ('parameters'):
                result.setdefault(section_name, {})
            else:
                result.setdefault(section_name, [])
            if len(node.children) == 1:
                continue
            elif len(node.children) == 2:
                node = node.children[1]
            else:
                raise Exception('Unexpected number of children for section')

        if not section_name:
            raise Exception("Extended reStructured text docs must provide content in subsections")
        
        if section_name == 'parameters':
            param_overrides = _process_definition_list(node)
            for param_override in param_overrides:
                result['parameters'][param_override['term']] =  {
                    'type': ' | '.join(param_override['classifiers']),
                    'definition': param_override['definition']
                }
        elif section_name == 'returns':
            param_overrides = _process_definition_list(node)
            # -- there really shouldn't be any classifiers in the return section, so we just reference the "term" and "definition"
            result['returns'] = [ (ref['term'], ref['definition']) for ref in param_overrides ]
            continue
        else:
            # Examples! The first element will be the section titile text, but the rest will be the content.
            # so we want to copy that into the new description details.
            result[section_name].append(node.astext())
        
    # join pieces
    return(result)
    
def _explore_nodes(node, indent = ""):
    """ A debugging function to analyse the structure of docutils generated xml trees. Do not use directly. """
    for child in node.children:             
        print(f"{indent}Child: {child.tagname}:")        
        if hasattr(child, 'attributes') and child.attributes:
            attrib = {key:val for key, val in child.attributes.items() if type(val) is not list or len(val)}
            if attrib:
                print(f"{indent}   Attrib: {attrib}")
        if child.children:            
            _explore_nodes(child, indent + "   ")