from dag import DAGNode, DAG
from commands import *
from pipeline_block import PipelineBlock

remove_existing_txt = PipelineBlock(
    command=Shell("rm *.txt", lambda _: True),
    postconditions=[Not(FilesExist("*.txt"))],
    failure_handler=Printer("Failed to remove *.txt"),
)

create_file_block = PipelineBlock(
    command=Shell("touch file.txt"),
    preconditions=[Not(FileExists("file.txt"))],
    postconditions=[FileExists("file.txt")],
    failure_handler=Shell("Failed to create file"),
)

create_other_file_block = PipelineBlock(
    command=Shell("touch other_file.txt"),
    preconditions=[Not(FileExists("other_file.txt"))],
    postconditions=[FileExists("other_file.txt")],
    failure_handler=Shell("Failed to create other file"),
)

delete_both_block = PipelineBlock(
    command=Shell("rm file.txt other_file.txt"),
    preconditions=[FileExists("file.txt"),
                   FileExists("other_file.txt")],
    postconditions=[
        Not(FileExists("file.txt")),
        Not(FileExists("other_file.txt"))
    ],
    failure_handler=Printer("Failed to delete files"),
)

remove_existing_txt_node = DAGNode("remove_existing_txt", remove_existing_txt)
create_file_node = DAGNode("create_file", create_file_block,
                           [remove_existing_txt_node.id])
create_other_file_node = DAGNode("create_other_file", create_other_file_block,
                                 [remove_existing_txt_node.id])
delete_both_node = DAGNode("delete_both", delete_both_block,
                           [create_file_node.id, create_other_file_node.id])

dag = DAG([
    remove_existing_txt_node, delete_both_node, create_file_node,
    create_other_file_node
])
dag_success, failed_node = dag.run()
print("Pipeline run result:", dag_success, "Failed node:", failed_node)
