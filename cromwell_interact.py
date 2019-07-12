#! /usr/bin/env python3
from subprocess import Popen,PIPE,call,run
import shlex,os,argparse,datetime,json,pyperclip
from utils import make_sure_path_exists
from collections import defaultdict
import re
import sys
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
        with open(metadat ,'w') as o:
            cmd1 = "curl -X GET \"http://localhost/api/workflows/v1/" + str(workflowID) + "/metadata?expandSubWorkflows=false\" -H \"accept: application/json\" --socks5 localhost:5000  "
            pr = Popen(shlex.split(cmd1), stdout=o, stderr=PIPE)

            while True:
                line=pr.stderr.read(1)
                if line.decode("ASCII") == '' and pr.poll() != None:
                    break
                sys.stdout.write(line.decode("ASCII"))
                sys.stdout.flush()

            if pr.returncode!=0:
                raise Exception(f'Error occurred while requesting metadata. Did you remember to setup ssh tunnel? Error:\n{pr.stderr}')

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
        uniq_shards={}
        for job in v:
            if call not in paths:
                paths[call] =  re.sub(r'shard-[0-9]*/stdout', '', job["stdout"])

            ## add the last attempt for each shard-1 because retryable failures and new tries are reported separately
            if job["shardIndex"] not in uniq_shards or int(job["attempt"])>int(uniq_shards[job["shardIndex"]]["attempt"]):
                uniq_shards[job["shardIndex"]]=job

        for job in uniq_shards.values():
            summaries[call][ f'{job["executionStatus"]}{ "_"+job["backendStatus"] if "backendStatus" in job else "" }' ]+=1
    return (summaries,paths)


def print_summary(metadat):
    summary = get_job_summary(metadat)
    print(f'Workflow name\t{ get_workflow_name(metadat) } ')
    times = get_workflow_exec_time(metadat)
    print(f'Start\t{times[0]} \nEnd\t{times[1]}')

    print(f'Current status \t { get_workflow_status(metadat)}')
    print("")
    for k,v in summary[0].items():
        callstat = ", ".join([ f'{stat}:{n}' for stat,n in v.items()])
        print(f'Call "{k}"\nBasepath\t{summary[1][k]}\njob statuses\t {callstat}\n')

def print_failed_jobs(metadata, args):
    failed_jobs = [ metadat ]

    fails = [ (c, [j for j in v if j["executionStatus"]=="Failed" ] ) for c,v in metadata["calls"].items() ]
    print("FAILED JOBS:")
    for call,v in fails:
        if len(v)==0:
            continue

        for j in v:
            print(f'Failed {call}\tcall#{j["shardIndex"]}')
            print(f'logpath\t{j["stdout"]}')
            fail_msgs = [ f['message'] for f in j["failures"] ]
            "\n\n".join(fail_msgs)


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
    parser_meta.add_argument("--failed_jobs", action="store_true"  ,help="Print summary of workflow")
    # abort parser
    parser_abort = subparsers.add_parser('abort' )
    parser_abort.add_argument("id", type= str,help="workflow id")

    parser_abort = subparsers.add_parser('connect' )
    parser_abort.add_argument("server", type=str,help="Cromwell server name")
    parser_abort.add_argument("--port", type=int, default=5000, help="SSH port")

    args = parser.parse_args()

    if args.outpath:
        rootPath=args.outpath + "/"

    if args.command =='abort':
        abort(args.id)
    elif args.command == "metadata":
        metadat = get_metadata(args)
        if args.summary:
            print_summary(metadat)
        if args.failed_jobs:
            print_failed_jobs(metadat, args)
    elif args.command == "submit":
        if not args.inputs:
            args.inputs = args.wdl.replace('.wdl','.json')
        print(args.wdl,args.inputs,args.label)
        submit(args.wdl,args.inputs,args.label)
    elif args.command == "connect":
        print("Trying to connect to server...")
        pr = Popen(f'gcloud compute ssh {args.server} -- -N -D localhost:{args.port} -o "ExitOnForwardFailure yes"', shell=True, stdout=PIPE ,stderr=PIPE, encoding="ASCII")
        pr.wait(5)

        print(pr.stdout.read())
        if pr.returncode!=0:
            raise Exception(f'Error occurred trying to connect. Error:\n{ pr.stderr.read()}')
        else:
            print("Connection opened")
