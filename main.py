from dag import DAGNode, DAG
from commands import *
from handlers import *
from pipeline_block import PipelineBlock

twilio_creds = Path().home() / "twilio_creds.txt"

remove_existing_txt = PipelineBlock(
    command=LocalShell("rm *.txt", lambda _: True),
    postconditions=[Not(FilesExist("*.txt"))],
    # failure_handler=TwilioHandler(twilio_creds, "Failed to remove *.txt"),
    success_handler=TwilioHandler(twilio_creds, "Successfully removed *.txt"),
)

create_file_block = PipelineBlock(
    command=LocalShell("touch file.txt"),
    preconditions=[Not(FileExists("file.txt"))],
    postconditions=[FileExists("file.txt")],
    # failure_handler=TwilioHandler(twilio_creds, "Failed to create file"),
    # success_handler=TwilioHandler(twilio_creds, "Successfully created file"),
)

create_other_file_block = PipelineBlock(
    command=LocalShell("touch other_file.txt"),
    preconditions=[Not(FileExists("other_file.txt"))],
    postconditions=[FileExists("other_file.txt")],
    # failure_handler=TwilioHandler(twilio_creds, "Failed to create other_file"),
    # success_handler=TwilioHandler(twilio_creds,
    #                               "Successfully created other_file"),
)

delete_both_block = PipelineBlock(
    command=LocalShell("rm file.txt other_file.txt"),
    preconditions=[FileExists("file.txt"),
                   FileExists("other_file.txt")],
    postconditions=[
        Not(FileExists("file.txt")),
        Not(FileExists("other_file.txt"))
    ],
    # failure_handler=TwilioHandler(twilio_creds, "Failed to delete both files"),
    # success_handler=TwilioHandler(twilio_creds,
    #                               "Successfully deleted both files"),
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
