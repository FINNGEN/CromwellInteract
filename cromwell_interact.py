from subprocess import Popen,PIPE,call
import shlex,os,argparse,datetime,json,pyperclip
from utils import make_sure_path_exists
from collections import defaultdict
import re

rootPath = '/'.join(os.path.realpath(__file__).split('/')[:-1]) + '/'
tmpPath =rootPath
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
    with open(os.path.join(rootPath,'workflows.log'),'a') as o:
        o.write(' '.join([current_date,jobID,label]) + '\n')


def workflowstatus(jsondat):
    return jsondat["status"]

def get_workflow_failures(jsondat):
    return [ m["message"] for m in d["failures"][0].values() ]

def get_metadata(args):
    workflowID = args.id
    if not args.file:
        metadat = f"{tmpPath}{workflowID}.json"
        with open(f"{tmpPath}{workflowID}.json" ,'w') as o:
            cmd1 = "curl -X GET \"http://localhost/api/workflows/v1/" + str(workflowID) + "/metadata?expandSubWorkflows=false\" -H \"accept: application/json\" --socks5 localhost:5000  "
            call(shlex.split(cmd1),stdout = o)
            print(cmd1)
            metadat = args.file
            print(f"Metadata saved to {tmpPath + workflowID}.json")
    else:
        metadat=args.file
    print(f'opening {metadat}' )
    return json.load(open(metadat,'r'))

def get_n_jobs(jsondat):
    pass
def get_n_running_jobs(jsondat):
    pass
def get_n_failed_jobs(jsondat):
    pass

def get_n_waiting_jobs(jsondat):
    pass

def get_workflow_exec_time(jsondat):
    return [jsondat["start"],jsondat["end"]]

def get_workflow_name(jsondat):
    return jsondat["workflowName"]

def get_workflow_status(jsondat):
    return jsondat['status']


def get_job_summary(jsondat):

    summaries = defaultdict( lambda: defaultdict(int) )

    paths = {}
    for call,v in jsondat["calls"].items():
        for job in v:
            if call not in paths:
                print(job["stdout"])
                paths[call] =  re.sub(r'shard-[0-9]*/stdout', '', job["stdout"])
            summaries[call][ f'{job["executionStatus"]}{ "_"+job["backendStatus"] if "backendStatus" in job else "" }' ]+=1

    return (summaries,paths)


def print_summary(metadat):
    summary = get_job_summary(metadat)
    print(f'Workflow name\t{ get_workflow_name(metadat) } ')
    times = get_workflow_exec_time(metadat)
    print(f'Start\t{times[0]} \nEnd\t{times[1]}')

    print(f'Current status \t { get_workflow_status(metadat)}')

    for k,v in summary[0].items():
        callstat = ", ".join([ f'{stat}:{n}' for stat,n in v.items()])
        print(f'Call "{k}"\nBasepath\t{summary[1][k]}\njob statuses\t {callstat}')

def abort(workflowID):
    cmd1 = "curl -X POST \"http://localhost/api/workflows/v1/" + str(workflowID) + "/abort\" -H \"accept: application/json\" --socks5 localhost:5000  "
    pr = subprocess.run(shlex.split(cmd1), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='ASCII' )
    print(cmd1)
    print(json.loads(pr.stdout))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Run Cromwell commands from command line")

    subparsers = parser.add_subparsers(help='help for subcommand',dest ="command")
    parser.add_argument('--outpath', type=str, help='Path to wdl script',required = False)
    # submit parser
    parser_submit = subparsers.add_parser('submit', help='submit a job')
    parser_submit.add_argument('--wdl', type=str, help='Path to wdl script',required = True)
    parser_submit.add_argument('--inputs', type=str, help='Path to wdl inputs')
    parser_submit.add_argument('--label', type=str, help='Label of the workflow',default = '')
    # metadata parser
    parser_meta = subparsers.add_parser('metadata')
    parser_meta.add_argument("id", type= str,help="workflow id")
    parser_meta.add_argument("--file", type=str  ,help="Use already downloaded meta json file as data")
    parser_meta.add_argument("--summary", action="store_true"  ,help="Print summary of workflow")
    # abort parser
    parser_abort = subparsers.add_parser('abort' )
    parser_abort.add_argument("id", type= str,help="workflow id")

    args = parser.parse_args()

    if args.outpath:
        rootPath=args.outpath + "/"

    if args.command =='abort':
        abort(args.id)

    if args.command == "metadata":
        metadat = get_metadata(args)
        if args.summary:
            print_summary(metadat)


    if args.command == "submit":
        if not args.inputs:
            args.inputs = args.wdl.replace('.wdl','.json')
        print(args.wdl,args.inputs,args.label)
        submit(args.wdl,args.inputs,args.label)
