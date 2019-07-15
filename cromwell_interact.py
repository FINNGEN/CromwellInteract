from subprocess import Popen,PIPE,call
import shlex,os,argparse,datetime,json,pyperclip
from utils import make_sure_path_exists

rootPath = '/'.join(os.path.realpath(__file__).split('/')[:-1]) + '/'
tmpPath =rootPath + 'tmp_path/'
make_sure_path_exists(tmpPath)


def submit(wdlPath,inputPath,label = ''):

    cmd = "curl -X POST \"http://localhost/api/workflows/v1\" -H \"accept: application/json\" -H \"Content-Type: multipart/form-data\" -F \"workflowSource=@"+wdlPath +"\" -F \"workflowInputs=@"+inputPath+";type=application/json\" --socks5 localhost:5000"
    stringCMD = shlex.split(cmd)
    #call(stringCMD)    

    proc = Popen(stringCMD, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode
    jobID = json.loads(out.decode())['id']
    pyperclip.copy(jobID)
    current_date = datetime.datetime.today().strftime('%Y-%m-%d')
    wdl_name = os.path.basename(wdlPath).split('.wdl')[0]
    with open(os.path.join(rootPath,'workflows.log'),'a') as o:
        o.write(' '.join([current_date,wdl_name,jobID,label]) + '\n')

    
def get_metadata(workflowID):
    with open(tmpPath + workflowID ,'w') as o:
        cmd1 = "curl -X GET \"http://localhost/api/workflows/v1/" + str(workflowID) + "/metadata?expandSubWorkflows=false\" -H \"accept: application/json\" --socks5 localhost:5000  "
        call(shlex.split(cmd1),stdout = o)        
    cmd2 = "python -m json.tool " + tmpPath + workflowID
    call(shlex.split(cmd2))
   
    print(cmd1 + "  |  " + cmd2 )

def abort(workflowID):
    with open(tmpPath + workflowID ,'w') as o:
        cmd1 = "curl -X POST \"http://localhost/api/workflows/v1/" + str(workflowID) + "/abort\" -H \"accept: application/json\" --socks5 localhost:5000  "
        call(shlex.split(cmd1),stdout = o)        
    cmd2 = "python -m json.tool " + tmpPath + workflowID
    call(shlex.split(cmd2))
   
    print(cmd1 + "  |  " + cmd2 )


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Run Cromwell commands from command line")
    subparsers = parser.add_subparsers(help='help for subcommand',dest ="command")

    # submit parser
    parser_submit = subparsers.add_parser('submit', help='submit a job')
    parser_submit.add_argument('--wdl', type=str, help='Path to wdl script',required = True)
    parser_submit.add_argument('--inputs', type=str, help='Path to wdl inputs')
    parser_submit.add_argument('--label', type=str, help='Label of the workflow',default = '')
    # metadata parser
    parser_meta = subparsers.add_parser('metadata')
    parser_meta.add_argument("id", type= str,help="workflow id")
    # abort parser
    parser_abort = subparsers.add_parser('abort' )
    parser_abort.add_argument("id", type= str,help="workflow id")

    parser_log = subparsers.add_parser('log', help='prints the log')
    parser_log.add_argument("--n", type= int,default =10,help="number of latest jobs to print")
    
    args = parser.parse_args()

    if args.command =='abort':
        abort(args.id)

    if args.command == "metadata":
        get_metadata(args.id)

    if args.command == "submit":
        if not args.inputs:
            args.inputs = args.wdl.replace('.wdl','.json')
        print(args.wdl,args.inputs,args.label)
        submit(args.wdl,args.inputs,args.label)
    

    if args.command == "log":
        with open(os.path.join(rootPath,'workflows.log'),'rt') as i:
            data = i.readlines()
        idx = min(args.n,len(data))
        for line in data[-idx:]: print(line.strip())
                

        
