import pytest
from jorvik.data_lineage import execution_plan
from pyspark.sql import SparkSession
from fixtures.data_lineage import *  # noqa: F401 F403


@pytest.mark.data_lineage
def test_capture_explain(spark: SparkSession) -> None:
    '''
    Test the capture_explain function to ensure it captures the execution plan correctly.
    '''
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    explain_result = execution_plan.capture_explain(df)

    # Check if the result is a string and contains '== Physical Plan =='
    assert isinstance(explain_result, str)
    assert '== Physical Plan ==' in explain_result


@pytest.mark.data_lineage
def test_split_formatted_explain(explain_result: str) -> None:
    exec_plan, node_details = execution_plan.split_formatted_explain(explain_result)
    assert '== Physical Plan ==' in exec_plan
    assert '(1) Scan parquet' in node_details


@pytest.mark.data_lineage
def test_parse_node_details(node_details_str: str) -> None:
    node_details_dict = execution_plan.parse_node_details(node_details_str)
    assert set(node_details_dict.keys()) == {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
    assert set(node_details_dict[1].keys()) == {'Output [11]', 'Batched', 'Location', 'ReadSchema'}
    assert node_details_dict[1]['Location'] == 'PreparedDeltaFileIndex [dbfs:/mnt/bronze/adventure_works/sales/data]'


@pytest.mark.data_lineage
def test_is_section_header() -> None:
    assert execution_plan.is_section_header('== Physical Plan ==')
    assert not execution_plan.is_section_header('(5) Filter')
    assert not execution_plan.is_section_header('')


@pytest.mark.data_lineage
def test_clean_identation_markers() -> None:
    line = '      :     +- Project (8)'
    cleaned_line = execution_plan.clean_indentation_markers(line)
    assert cleaned_line == '               Project (8)'


@pytest.mark.data_lineage
def test_get_indentation_level() -> None:
    line = '               Project (8)'
    level = execution_plan.get_indentation_level(line, n_spaces=3)
    assert level == 5


@pytest.mark.data_lineage
def test_parse_node() -> None:
    line = '* ShuffleQueryStage (11), Statistics(sizeInBytes=1669.9 MiB)'
    node_id, node_name = execution_plan.parse_node(line)
    assert node_id == 11
    assert node_name == 'ShuffleQueryStage'


@pytest.mark.data_lineage
def test_parse_execution_plan(execution_plan_str: str) -> None:
    expected_node_data = [(1, 'Scan parquet', 7),
                          (2, 'Filter', 6),
                          (3, 'Project', 5),
                          (4, 'Exchange', 4),
                          (5, 'Sort', 3),
                          (6, 'Scan ExistingRDD', 6),
                          (7, 'Filter', 5),
                          (8, 'Exchange', 4),
                          (9, 'Sort', 3),
                          (10, 'SortMergeJoin LeftOuter', 2),
                          (11, 'Project', 1),
                          (12, 'AdaptiveSparkPlan', 0)]
    flat_nodes = execution_plan.parse_execution_plan(execution_plan_str)
    assert len(flat_nodes) == 12
    for node in flat_nodes:
        assert isinstance(node, execution_plan.ExecutionNode)
        assert (node.id, node.name, node.height) in expected_node_data


@pytest.mark.data_lineage
def test_build_execution_tree(explain_result: str) -> None:
    root = execution_plan.build_execution_tree(explain_result)

    # Check root node
    assert root.id == 12
    assert root.name == 'AdaptiveSparkPlan'
    assert root.height == 0

    # Check children of root
    assert len(root.children) == 1
    child = root.children[0]
    assert child.id == 11
    assert child.name == 'Project'
    assert child.height == 1


@pytest.mark.data_lineage
def test_execution_node_add_child() -> None:
    parent = execution_plan.ExecutionNode(1, 'Parent', 0)
    child = execution_plan.ExecutionNode(2, 'Child', 1)
    parent.add_child(child)

    assert len(parent.children) == 1
    assert parent.children[0] == child


@pytest.mark.data_lineage
def test_execution_node_set_properties() -> None:
    node = execution_plan.ExecutionNode(1, 'Node', 0)
    properties = {'key1': 'value1', 'key2': 'value2'}
    node.set_properties(properties)

    assert node.properties == properties


@pytest.mark.data_lineage
def test_execution_node_find_leaves(explain_result: str) -> None:
    root = execution_plan.build_execution_tree(explain_result)

    leaf_nodes = root.leaves
    assert len(leaf_nodes) == 2
    assert {node.id for node in leaf_nodes} == {1, 6}


@pytest.mark.data_lineage
def test_execution_node_print_tree(capsys) -> None:
    '''
    Test the print_tree method of ExecutionNode to ensure the tree structure is printed correctly.
    '''
    root = execution_plan.ExecutionNode(1, 'Root', 0)
    child = execution_plan.ExecutionNode(2, 'Child', 1)
    root.add_child(child)

    root.print_tree()
    captured = capsys.readouterr()
    assert captured.out == "Root (1)\n  Child (2)\n"
