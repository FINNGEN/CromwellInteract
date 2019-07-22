#! /usr/bin/env python3
from subprocess import Popen,PIPE,call,run
import subprocess
import shlex,os,argparse,datetime,json,pyperclip
from utils import make_sure_path_exists
from collections import defaultdict
import re
import sys
rootPath = '/'.join(os.path.realpath(__file__).split('/')[:-1]) + '/'
tmpPath = os.path.join(rootPath,'tmp')
make_sure_path_exists(tmpPath)


def submit(wdlPath,inputPath,label = '', dependencies=None):

    cmd = (f'curl -X POST "http://localhost/api/workflows/v1" -H "accept: application/json" -H "Content-Type: multipart/form-data" '
           f' -F "workflowSource=@{wdlPath}" -F "workflowInputs=@{inputPath};type=application/json" --socks5 localhost:5000'
          )

    if dependencies is not None:
        cmd = f'{cmd} -F \"workflowDependencies=@{dependencies};type=application/zip"'

    #call(stringCMD)
    stringCMD = shlex.split(cmd)
    proc = Popen(stringCMD, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode
    if exitcode!=0:
        raise Exception(f'Error while submitting job. Error:\n{err}')
    resp = json.loads(out.decode())
    jobID = resp['id']
    pyperclip.copy(jobID)
    print(resp)
    current_date = datetime.datetime.today().strftime('%Y-%m-%d')
    with open(os.path.join(rootPath,'workflows.log'),'a') as o:
        o.write(' '.join([current_date,jobID,label]) + '\n')


def workflowstatus(jsondat):
    return jsondat["status"]

def get_workflow_failures(jsondat):
    return [ m["message"] for m in d["failures"][0].values() ]

def get_metadata(id):
    workflowID = id

    metadat = f"{os.path.join(tmpPath,workflowID +'json')}"
    with open(metadat ,'w') as o:
        cmd1 = "curl -X GET \"http://localhost/api/workflows/v1/" + str(workflowID) + "/metadata?expandSubWorkflows=false\" -H \"accept: application/json\" --socks5 localhost:5000  "

        # interim output looks ugly in case of sub workflows
        #while True:
        #    line=pr.stderr.read(1)
        #    if line.decode("ASCII") == '' and pr.poll() != None:
        #        break
        #    sys.stdout.write(line.decode("ASCII"))
        #    sys.stdout.flush()

        pr = subprocess.run(shlex.split(cmd1), stdout=o, stderr=PIPE, encoding="ASCII")
        if pr.returncode!=0:
            print(pr.stderr)
            raise Exception(f'Error occurred while requesting metadata. Did you remember to setup ssh tunnel? Use cromwellinteract.py connect servername')
        print(f"Metadata saved to {metadat}")
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
    return [jsondat["start"] if "start" in jsondat else "NA",jsondat["end"] if "end" in jsondat else "NA"]

def get_workflow_name(jsondat):
    return jsondat["workflowName"]

def get_workflow_status(jsondat):
    return jsondat['status']


def get_workflow_summary(jsondat):
    summaries = defaultdict( lambda: dict() )
    paths = {}
    for call,v in jsondat["calls"].items():
        uniq_shards={}
        summaries[call]['jobstats'] = {}
        summaries[call]['failed_jobs'] = []
        for job in v:
            if job["shardIndex"] not in uniq_shards or int(job["attempt"])>int(uniq_shards[job["shardIndex"]]["attempt"]):
                uniq_shards[job["shardIndex"]]=job

        for job in uniq_shards.values():
            stat_str = f'{job["executionStatus"]}{ "_"+job["backendStatus"] if "backendStatus" in job else "" }'
            if stat_str not in summaries[call]['jobstats']:
               summaries[call]['jobstats'][stat_str]=0
            summaries[call]['jobstats'][stat_str]+=1

            if job["executionStatus"]=="Failed":
                summaries[call]['failed_jobs'].append(job)

            if "subWorkflowId" not in job:
                summaries[call]["basepath"] = re.sub(r"(((shard|attempt)-[0-9]+/)+stdout|/stdout)","",job["stdout"])
            else:
                summaries[call]['subworkflowid'] = job["subWorkflowId"]

    return (summaries)


def ind(n):
    return "\t".join([""]*(n+1))


def print_summary(metadat, args,indent=0):
    summary = get_workflow_summary(metadat)

    print(f'{ind(indent)}Workflow name\t{ get_workflow_name(metadat) } ')
    print(f'{ind(indent)}Current status \t { get_workflow_status(metadat)}')
    times =get_workflow_exec_time(metadat)
    print(f'{ind(indent)}Start\t{times[0]} \n{ind(indent)}End\t{times[1]}')
    print("")
    for k,v in summary.items():
        callstat = ", ".join([ f'{stat}:{n}' for stat,n in v['jobstats'].items()])
        #print(f'{ind(indent)}\n--------------')
        print(f'{ind(indent)}Call "{k}"\n{ind(indent)}Basepath\t{v["basepath"] if "basepath" in v else "sub-workflow" }\n{ind(indent)}job statuses\t {callstat}')

        if args.failed_jobs:
            print_failed_jobs(v["failed_jobs"], indent=indent)
        print("")
        if 'subworkflowid' in v:
            sub=get_metadata(v["subworkflowid"])
            print(f'{ind(indent)}Sub-workflow:')
            print_summary(sub, args, indent+1)


def get_failmsg(failure):
    while len(failure["causedBy"])>0:
        failure = failure["causedBy"][0]
    return failure["message"]

def print_failed_jobs(joblist, indent=0):
    print(f'{ind(indent)}FAILED JOBS:')
    if len(joblist)==0:
        print(f'{ind(indent)}No failed jobs!\n')
        return

    for j in joblist:
        print(f'{ind(indent)}Failed\tshard# {j["shardIndex"]}')
        # nested caused bys in subworkflows
        fail_msgs = [ get_failmsg(f) for f in j["failures"] ]
        #for f in j["failures"]:
        #    while len(f["causedBy"])>0:
        #        f=f["causedBy"]
        #        print(f)
        #        fail_msgs.append(f["message"])

        print("{}{}".format(ind(indent),"\n\n".join(fail_msgs)))


def abort(workflowID):
    cmd1 = "curl -X POST \"http://localhost/api/workflows/v1/" + str(workflowID) + "/abort\" -H \"accept: application/json\" --socks5 localhost:5000  "
    pr = subprocess.run(shlex.split(cmd1), stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='ASCII' )
    if pr.returncode!=0:
        print("Error occurred while submitting abort command to cromwell")
        print(pr.stderr)
    else:
        #print(cmd1)
        print(json.loads(pr.stdout))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Run Cromwell commands from command line")

    subparsers = parser.add_subparsers(help='help for subcommand',dest ="command")
    parser.add_argument('--outpath', type=str, help='Path to wdl script',required = False)
    # submit parser
    parser_submit = subparsers.add_parser('submit', help='submit a job')
    parser_submit.add_argument('--wdl', type=str, help='Path to wdl script',required = True)
    parser_submit.add_argument('--inputs', type=str, help='Path to wdl inputs')
    parser_submit.add_argument('--deps', type=str, help='Path to zipped dependencies file')
    parser_submit.add_argument('--label', type=str, help='Label of the workflow',default = '')
    # metadata parser
    parser_meta = subparsers.add_parser('metadata', help="Requests metadata and summaries of workflows")
    parser_meta.add_argument("id", type= str,help="workflow id")
    parser_meta.add_argument("--file", type=str  ,help="Use already downloaded meta json file as data")
    parser_meta.add_argument("--summary", action="store_true"  ,help="Print summary of workflow")
    parser_meta.add_argument("--failed_jobs", action="store_true"  ,help="Print summary of failed jobs")
    # abort parser
    parser_abort = subparsers.add_parser('abort' )
    parser_abort.add_argument("id", type= str,help="workflow id")

    parser_abort = subparsers.add_parser('connect' )
    parser_abort.add_argument("server", type=str,help="Cromwell server name")
    parser_abort.add_argument("--port", type=int, default=5000, help="SSH port")

    # logging parser
    parser_log = subparsers.add_parser('log', help='prints the log')
    parser_log.add_argument("--n", type= int,default =10,help="number of latest jobs to print")

    args = parser.parse_args()

    if args.outpath:
        rootPath=args.outpath + "/"

    if args.command =='abort':
        abort(args.id)
    elif args.command == "metadata":
        if args.file:
            metadat=json.load(open(args.file))
        else:
            metadat = get_metadata(args.id)

        if args.summary or args.failed_jobs:
            print_summary(metadat, args=args)

    elif args.command == "submit":
        if not args.inputs:
            args.inputs = args.wdl.replace('.wdl','.json')
        print(args.wdl,args.inputs,args.label)
        submit(args.wdl,args.inputs,args.label, args.deps)
    elif args.command == "connect":
        print("Trying to connect to server...")
        subprocess.check_call(f'gcloud compute ssh {args.server} -- -f -n -N -D localhost:{args.port} -o "ExitOnForwardFailure yes"',
                    shell=True, encoding="ASCII")

        print(f'Connection opened to {args.server} via localhost:{args.port}')

    if args.command == "log":
        with open(os.path.join(rootPath,'workflows.log'),'rt') as i:
            data = i.readlines()
        idx = min(args.n,len(data))
        for line in data[-idx:]: print(line.strip())
