'''
This module provides functions to capture physical execution plan a DataFrame in PySpark
and parse it into a tree structure.
'''
import io
import sys
import re
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame

class ExecutionNode():
    '''
    Initialize an ExecutionNode object.
    Attributes:
        id (int): Unique identifier for the node. Example: 11
        name (str): Name of the node. Examples: 'Filter', 'Scan parquet', 'Project', etc.
        height (int): Level of the node in the execution plan tree, identified by indentation.
        properties (dict): Additional properties of the node.
    '''
    def __init__(self, id: int, name: str, height: int, properties: Dict[str, str] = None) -> None:
        self.id = id
        self.name = name
        self.height = height
        self.properties = properties if properties else {}
        self.children = []

    def add_child(self, node):
        self.children.append(node)

    def set_properties(self, properties: Dict[str, str]) -> None:
        self.properties = properties

    @property
    def leaves(self) -> List['ExecutionNode']:
        '''
        Recursively find all leaf nodes within the children of the current node.
        Leaf nodes are nodes that do not have any children.
        '''
        if not self.children:
            return [self]
        leaf_nodes = []
        for child in self.children:
            leaf_nodes.extend(child.leaves)
        return leaf_nodes

    def print_tree(self, level: int = 0, print_properties: bool = False) -> None:
        '''Print the execution plan tree structure starting from the current node.'''
        print("  " * level + f"{self.name} ({self.id})")
        if print_properties:
            for key, value in self.properties.items():
                print("  " * (level + 1) + f"{key}: {value}")
        for child in self.children:
            child.print_tree(level + 1, print_properties)

# ==================== Capture df.explain ====================
def capture_explain(df: DataFrame) -> str:
    '''
    pyspark.sql.DataFrame.explain only prints the explain output to stdout.
    This function captures output of df.explain("formatted") and returns it as a string.
    '''
    old_stdout = sys.stdout
    new_stdout = io.StringIO()
    sys.stdout = new_stdout

    try:
        df.explain('formatted')
    finally:
        sys.stdout = old_stdout

    explain_result = new_stdout.getvalue()
    return explain_result

def split_formatted_explain(explain_result: str) -> List[str]:
    '''
    Split the formatted explain result into 2 sections: execution plan (tree-like structure) and node details
    '''
    exec_plan, node_details = explain_result.split('\n\n\n')[:2]
    return exec_plan, node_details

# ==================== Parse node details ====================

def parse_node_details(node_details: str) -> Dict[int, Dict]:
    '''
    Parse the node details section to extract additional information about each node.
    '''
    node_texts = [n for n in node_details.split('\n\n') if len(n.strip()) > 0]
    node_details_dict = {}
    for node_text in node_texts:
        lines = node_text.split('\n')
        node_id = int(lines[0].split(' ')[0].replace("(", "").replace(")", ""))
        node_info = {}
        for line in lines[1:]:
            if line.startswith('+-'):  # Hit a nested plan, usually seen when a dataframe is cached with df.cache()
                break

            try:
                key, value = line.split(':', 1)
            except ValueError:  # If line does not have key: value format, skip it
                continue
            node_info[key.strip()] = value.strip()
        node_details_dict[node_id] = node_info
    return node_details_dict

# ==================== Parse physical execution plan ====================

def is_section_header(line: str) -> bool:
    '''
    Check if the line is a section header in the execution plan.
    For example, "== Physical Plan ==" or "== Current Plan =="
    '''
    pattern = r'^\s*==\s*[^=]+\s*==\s*$'
    return bool(re.match(pattern, line))

def clean_indentation_markers(line: str) -> str:
    '''
    Replace indentation markers +, -, : with white space.
    '''
    return re.sub(r'[+\-:]', ' ', line)

def get_indentation_level(line: str, n_spaces: int = 3) -> int:
    '''
    Get the indentation level of the line based on the number of leading white space characters
    '''
    return (len(line) - len(line.lstrip())) // n_spaces

def parse_node(line: str) -> Tuple:
    '''
    Parse a line of the execution plan to extract node id and node name.
    Example: '* ShuffleQueryStage (11), Statistics(sizeInBytes=1669.9 MiB)' is parsed to (11, 'ShuffleQueryStage')
    '''
    int_match = re.search(r'\((\d+)\)', line)  # Match the number in parentheses
    node_id = int(int_match.group(1)) if int_match else None

    name_match = re.search(r"[a-zA-Z0-9][a-zA-Z0-9\s]*?(?=\s*\()", line)  # First alphanumeric string before '('
    node_name = name_match.group(0).strip() if name_match else None
    return node_id, node_name

# ==================== Build physical execution plan as tree structure ====================

def parse_execution_plan(execution_plan: str) -> List[ExecutionNode]:
    '''
    Parse the execution plan into a list of nodes.
    '''
    flat_nodes = []
    lines = [clean_indentation_markers(line) for line in execution_plan.split('\n')]
    for line in lines:
        if is_section_header(line):
            continue
        indentation = get_indentation_level(line)
        node_id, node_name = parse_node(line)
        flat_nodes.append(ExecutionNode(node_id, node_name, indentation))
    return flat_nodes

def build_execution_tree(explain_result: str) -> ExecutionNode:
    '''
    Build a tree structure from result of df.explain("formatted").
    Parent - child relationship is established based on indentation level.
    Returns the root node of the tree.
    '''
    exec_plan, node_details = split_formatted_explain(explain_result)
    flat_nodes = parse_execution_plan(exec_plan)
    node_details_dict = parse_node_details(node_details)

    stack = []
    root = None

    for node in flat_nodes:
        node.set_properties(node_details_dict[node.id])
        while stack and stack[-1].height >= node.height:
            stack.pop()

        if stack:
            stack[-1].add_child(node)
        elif root:
            raise ValueError(f"Execution graph contains multiple roots: {root.name} and {node.name}")
        else:
            root = node

        stack.append(node)
    return root
