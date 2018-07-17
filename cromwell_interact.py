from subprocess import Popen,PIPE,call
import shlex
import os
import argparse
from utils import make_sure_path_exists


rootPath = '/'.join(os.path.realpath(__file__).split('/')[:-1]) + '/'
tmpPath =rootPath + 'tmp_path/'
make_sure_path_exists(tmpPath)


def submit(wdlPath,inputPath):

    cmd = "curl -X POST \"http://localhost/api/workflows/v1\" -H \"accept: application/json\" -H \"Content-Type: multipart/form-data\" -F \"workflowSource=@"+wdlPath +"\" -F \"workflowInputs=@"+inputPath+";type=application/json\" --socks5 localhost:5000"
    call(shlex.split(cmd))
    print("")

def get_metadata(workflowID):
    with open(tmpPath + workflowID ,'w') as o:
        cmd1 = "curl -X GET \"http://localhost/api/workflows/v1/" + str(workflowID) + "/metadata?expandSubWorkflows=false\" -H \"accept: application/json\" --socks5 localhost:5000  "
        call(shlex.split(cmd1),stdout = o)        
    cmd2 = "python -m json.tool " + tmpPath + workflowID
    call(shlex.split(cmd2))
   
    print(cmd1 + "  |  " + cmd2 +'/n')


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Run Cromwell commands from command line")
    subparsers = parser.add_subparsers(help='help for subcommand',dest ="command")

    # create the parser for the "command_1" command
    parser_submit = subparsers.add_parser('submit', help='submit a job')
    parser_submit.add_argument('--wdl', type=str, help='Path to wdl script',required = True)
    parser_submit.add_argument('--inputs', type=str, help='Path to wdl inputs',required = True)

    # create the parser for the "command_2" command
    parser_meta = subparsers.add_parser('metadata', help='help for command_2')
    parser_meta.add_argument("--id", type= str,help="workflow id")

    args = parser.parse_args()

    if args.command == "metadata":
        get_metadata(args.id)

    if args.command == "submit":
        submit(args.wdl,args.inputs)
    
