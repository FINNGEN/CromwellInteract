#! /usr/bin/env python3
from subprocess import Popen,PIPE,call,run
import subprocess
import shlex,os,argparse,datetime,json,pyperclip
from utils import make_sure_path_exists
from collections import defaultdict, Counter
import re,sys,warnings
rootPath = '/'.join(os.path.realpath(__file__).split('/')[:-1]) + '/'
tmpPath = os.path.join(rootPath,'tmp')
make_sure_path_exists(tmpPath)
import dateutil.parser
import requests
import json
import re

def process_inputs(args):

    if not args.inputs: args.inputs = args.wdl.replace('.wdl','.json')


    # labels and options are now mutually exclusive by structure
    if args.google_labels:
        labs = { labs[0]:labs[1] for labs in [ l.split("=") for l in args.google_labels.split(",") ] }
        wf_opts = {"google_labels":labs}

    if args.options:
        wf_opts = json.load(open(args.options,'r'))

    #monitoring by monitoring script
    if not args.disable_monitoring:
        wf_opts["monitoring_script"]=args.monitor


    if "product" not in wf_opts["google_labels"]:
        raise Exception("You must add product google label with --l product=value or --options json")

    return wf_opts

def submit(wdlPath,inputPath,port,wf_opts,label = '', dependencies=None, options=None, http_port=80):

    print(f'submitting {wdlPath}')
    ## force labeling:

    workflowname=""
    with open(wdlPath, 'r')  as wd:
        for l in wd:
            l=l.strip()
            if l.startswith("workflow"):
                workflowname=re.search('^workflow[ ]+([A-Za-z]+)',l).group(1)
                break

    user = subprocess.run('gcloud auth list --filter=status:ACTIVE --format="value(account)"', shell=True, stdout=subprocess.PIPE).stdout.decode().strip()
    wf_opts["google_labels"]["cromwell-submitter"]=user.replace("@","-at-").replace(".","-dot-")
    wf_opts["google_labels"]["cromwell-workflow-name"]=workflowname


    cmd = (f'curl -X POST http://localhost:{http_port}/api/workflows/v1 -H "accept: application/json" -H "Content-Type: multipart/form-data" '
           f' -F workflowSource=@"{wdlPath}";type=application/json --socks5 localhost:{port}'
           f' -F workflowOptions=\'{json.dumps(wf_opts)}\''
          )

    if inputPath is not None:
        cmd = f'{cmd} -F workflowInputs=@"{inputPath}"'

    if dependencies is not None:
        cmd = f'{cmd} -F \"workflowDependencies=@{dependencies};type=application/zip"'



    stringCMD = shlex.split(cmd)

    proc = Popen(stringCMD, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode
    if exitcode!=0:
        raise Exception(f'Error while submitting job. Error:\n{err}')
    resp = json.loads(out.decode())

    if resp['status']=='fail' or resp['status']=='Failed':
        raise Exception(f'Error in Cromwell request. Error:{resp["message"]}' )
    jobID = resp['id']
    print(jobID)
    pyperclip.copy(jobID)

    current_date = datetime.datetime.today().strftime('%Y-%m-%d')
    wdl_name = os.path.basename(wdlPath).split('.wdl')[0]
    if not label:label = wdl_name
    with open(os.path.join(rootPath,'workflows.log'),'a') as o:
        o.write(' '.join([current_date,wdl_name,jobID,label]) + '\n')


def workflowstatus(jsondat):
    return jsondat["status"]

def get_workflow_failures(jsondat):
    return [ m["message"] for m in d["failures"][0].values() ]

def get_metadata(id, port,timeout=60, nocalls=False, minkeys=False,http_port=80):
    workflowID = id

    metadat = f"{os.path.join(tmpPath,workflowID +'.json')}"
    with open(metadat ,'w') as o:
        excl_calls = ""
        if nocalls:
            excl_calls="&excludeKey=calls"

        keys=""
        if minkeys:
            keys=("&includeKey=status&includeKey=executionStatus&includeKey=failures&includeKey=workflowName")
                #"&includeKey=start&includeKey=end")

        cmd1 = f'curl -X GET \"http://localhost:{http_port}/api/workflows/v1/{workflowID}/metadata?expandSubWorkflows=false{excl_calls}{keys}\" -H \"accept: application/json\" --socks5 localhost:{port}  '
        print(cmd1)

        pr = subprocess.run(shlex.split(cmd1), stdout=o, stderr=PIPE, encoding="ASCII", timeout=timeout)
        if pr.returncode!=0:
            print(pr.stderr)
            raise Exception(f'Error occurred while requesting metadata. Did you remember to setup ssh tunnel? Use cromwellinteract.py connect servername')
        print(f"Metadata saved to {metadat}", file=sys.stderr)

    ret = json.load(open(metadat,'r'))
    if ret['status']=='fail' :
        raise Exception(f'Error requesting metadata. Cromwell message: {ret["message"]}')

    #if ret['status']=='Failed':
    #    raise Exception(f'Workflow not submitted successfully. Cromwell message: { ret["failures"]}')

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


def get_workflow_summary(jsondat, store_with_status=None):
    summaries = defaultdict( lambda: dict() )
    summary= defaultdict(lambda: dict())
    paths = {}

    for call,v in jsondat["calls"].items():
        uniq_shards={}
        for job in v:
            if job["shardIndex"] not in uniq_shards or int(job["attempt"])>int(uniq_shards[job["shardIndex"]]["attempt"]):
                uniq_shards[job["shardIndex"]]=job
        summary[call]['jobstats']= Counter()
        summary[call]['failed_jobs']=[]

        summary[call][store_with_status]=[]

        summary[call]['min_time']=None
        summary[call]['max_time']=None
        summary[call]['min_job']=None
        summary[call]['max_job']=None
        summary[call]['failed_jobs']=[]
        summary[call]['finished_jobs']=0
        summary[call]['total_time']=0

        for i,job in uniq_shards.items():
            summaries[f'{call}_{i}']['jobstats'] = Counter()
            summaries[f'{call}_{i}']['failed_jobs'] = []
            summaries[f'{call}_{i}'][store_with_status] = []

            stat_str = f'{job["executionStatus"]}{ "_"+job["backendStatus"] if "backendStatus" in job else "" }'

            if 'start' in job and 'end' in job:
                duration = (dateutil.parser.parse(job['end']) -dateutil.parser.parse( job['start'] )).total_seconds()
                summary[call]['finished_jobs']+=1
                if summary[call]['min_time'] is None or duration < summary[call]['min_time']:
                    summary[call]['min_time'] = duration
                    if 'stdout' in job:
                        summary[call]['min_job'] = job['stdout'] if 'stdout' in job else "shard:"+ str(job["shardIndex"])

                if summary[call]['max_time'] is None or  duration > summary[call]['max_time']:
                    summary[call]['max_time'] = duration


                    summary[call]['max_job'] = job['stdout'] if 'stdout' in job else "shard:"+ str(job["shardIndex"])

                summary[call]['total_time']+=duration

            summaries[f'{call}_{i}']['jobstats'][stat_str]+=1
            summary[f'{call}']['jobstats'][stat_str]+=1
            if job["executionStatus"]=="Failed":
                summaries[f'{call}_{i}']['failed_jobs'].append(job)
                summary[call]['failed_jobs'].append(job)

            if job["executionStatus"]==store_with_status:

                summaries[f'{call}_{i}'][store_with_status].append(job)
                summary[call][store_with_status].append(job)

            if call=="finemap.ldstore_finemap":
                print(job)
            if "subWorkflowId" not in job:
                if "stdout" in job:
                    summaries[f'{call}_{i}']["basepath"] = re.sub(r"(((shard|attempt)-[0-9]+/)+stdout|/stdout)","",job["stdout"])
                    summary[call]['basepath']= re.sub(r"(((shard|attempt)-[0-9]+/)+stdout|/stdout)","",job["stdout"])
            else:
                print(f'sub found for {call}_{i}')
                summaries[f'{call}_{i}']['subworkflowid'] = job["subWorkflowId"]

    return (summary,summaries)

def ind(n):
    return "\t".join([""]*(n+1))

def get_jobs_with_status(jsondat, status):
    return [ v for (c,v) in jsondat["calls"].items() if v["executionStatus"]==status ]

def print_summary(metadat, args, port, indent=0, expand_subs=False, timeout=60):
    summary,summaries = get_workflow_summary(metadat, args.print_jobs_with_status)
    print(f'{ind(indent)}Workflow name\t{ get_workflow_name(metadat) } ')
    print(f'{ind(indent)}Current status \t { get_workflow_status(metadat)}')
    times =get_workflow_exec_time(metadat)
    print(f'{ind(indent)}Start\t{times[0]} \n{ind(indent)}End\t{times[1]}')
    print("")

    top_call_counts = defaultdict(lambda :Counter())

    for k,v in summary.items():
        callstat = ", ".join([ f'{stat}:{n}' for stat,n in v['jobstats'].items()])
        totaljobs= 0
        for stat, n in v['jobstats'].items():
            top_call_counts[k][stat]+=n
            totaljobs +=n

        print(f'{ind(indent)}Call "{k}"\n{ind(indent)}Basepath\t{v["basepath"] if "basepath" in v else "sub-workflow" }\n{ind(indent)}job statuses\t {callstat}')
        max = f'{v["max_time"]/60.0:.2f}' if v["max_time"] is not None else None
        min = f'{v["min_time"]/60.0:.2f}' if v["min_time"] is not None else None
        avg = f'{v["total_time"]/v["finished_jobs"]/60.0:.2f}' if v["finished_jobs"]>0 else None
        print(f'{ind(indent)}Max time: {max} minutes, min time {min} minutes , average time { avg } minutes')
        print(f'{ind(indent)}Max job {v["max_job"]}\n{ind(indent)}Min job {v["min_job"]}')
        if args.failed_jobs:
            print_failed_jobs(v["failed_jobs"], indent=indent)

        if args.print_jobs_with_status:
            print_jobs_with_status(v[args.print_jobs_with_status],args.print_jobs_with_status, indent=indent)

        print("")
    for k,v in summaries.items():

        if 'subworkflowid' in v:
            print(f'{ind(indent)}Sub-workflow ({v["subworkflowid"]}):')
            if expand_subs:
                print("getting sub data")
                sub=get_metadata(v["subworkflowid"], port=port, timeout=timeout,
                            nocalls=args.no_calls, minkeys=args.minkeys, http_port=args.http_port)
                (top, summ) = print_summary(sub, args, port=port,indent=indent+1, expand_subs=expand_subs)
                for call,count in top.items():
                    if call in top_call_counts:
                        top_call_counts[call] = top_call_counts[call] + count
                        summary[call]['failed_jobs'].extend(summ[call]['failed_jobs'])
                        summary[call][args.print_jobs_with_status].extend(summ[call][args.print_jobs_with_status])
                    else:
                        top_call_counts[call] = count
                        summary[call]['failed_jobs'] = summ[call]['failed_jobs']
                        summary[call][args.print_jobs_with_status] = summ[call][args.print_jobs_with_status]

    return (top_call_counts,summary)

def get_failmsg(failure):
    while len(failure["causedBy"])>0:
        failure = failure["causedBy"][0]
    return failure["message"]

def print_jobs_with_status(joblist, status ,indent=0):
    print(f'{ind(indent)}Jobs with status {status}:')
    if len(joblist)==0:
        print(f'{ind(indent)}No jobs with status {status}!\n')
        return

    for j in joblist:
        print(j)
        print(f'{ind(indent)}Job \tshard# {j["shardIndex"]}')

def print_failed_jobs(joblist, indent=0):
    print(f'{ind(indent)}FAILED JOBS:')
    if len(joblist)==0:
        print(f'{ind(indent)}No failed jobs!\n')
        return

    for j in joblist:
        print(f'{ind(indent)}Failed\tshard# {j["shardIndex"]}')
        # nested caused bys in subworkflows
        fail_msgs = [ get_failmsg(f) for f in j["failures"] ]
        print("{}{}".format(ind(indent),"\n\n".join(fail_msgs)))


def abort(workflowID, port, http_port=80):
    #cmd1 = f'curl -X GET \"http://localhost/api/workflows/v1/{workflowID}/metadata?expandSubWorkflows=false\" -H \"accept: application/json\" --socks5 localhost:{port}  '
    cmd1 = f'curl -X POST \"http://localhost:{http_port}/api/workflows/v1/{workflowID}/abort\" -H \"accept: application/json\" --socks5 localhost:{port}'
    pr = subprocess.run(shlex.split(cmd1), stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='ASCII' )
    if pr.returncode!=0:
        print("Error occurred while submitting abort command to cromwell")
        print(pr.stderr)
    else:
        #print(cmd1)
        print(json.loads(pr.stdout))

def get_last_job():

    workflows = os.path.join(rootPath,'workflows.log')
    if not os.path.isfile(workflows):
        raise ValueError("NO WORKFLOWS DETECTED, PLEASE SPECIFY ID")

    else:
        with open(workflows,'rt') as i:
            for line in i:pass
        return line.strip().split(' ')[2]

def print_top_level_failure( metadat ):
    def print_all_failures (fails):
        if len(fails["causedBy"])==0:
            print(fails["message"])
            return
        for cause in fails["causedBy"]:
            print_all_failures(cause)

    for f in metadat["failures"]:
        print_all_failures(f)

def get_status(id, port,timeout=60, nocalls=False, minkeys=False,http_port=80):
    workflowID = id
    cmd1 = f'curl -X GET \"http://localhost:{http_port}/api/workflows/v1/{workflowID}/status\" -H \"accept: application/json\" --socks5 localhost:{port}  '
    print(cmd1)

    pr = subprocess.run(shlex.split(cmd1), capture_output=True, text=True, encoding="ASCII", timeout=timeout)
    if pr.returncode!=0:
        print(pr.stderr)
        raise Exception(f'Error occurred while requesting metadata. Did you remember to setup ssh tunnel? Use cromwellinteract.py connect servername')

    res = json.loads(pr.stdout)
    print(res)
    status = res['status']
    return status

def update_log(args,id,status):
    with open(args.workflow_log, "r+") as f:
        old = [elem.strip().split(" ") for elem in f.readlines()] # read everything in the file
        new_lines = []
        for line in old:
            date,name,w_id,*_ = line
            if w_id == id:
                if len(line) ==4:
                    line.append(status)
                elif len(line) == 5:
                    line[4] = status
                elif len(line) ==3:
                    line.append(name)
                    line.append(status)
            new_lines.append(line)
    with open(args.workflow_log,'wt') as o:
        for line in new_lines:
            o.write(' '.join(line) + '\n')



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Cromwell commands from command line")

    subparsers = parser.add_subparsers(help='help for subcommand',dest ="command")
    parser.add_argument('--outpath', type=str, help='Path to wdl script',required = False)
    parser.add_argument("--port", type=int, default=5000, help="SSH port")
    parser.add_argument("--http_port", type=int, default=80, help="Cromwell server port")

    parser_submit = subparsers.add_parser('submit', help='submit a job')
    parser_submit.add_argument('--wdl', type=str, help='Path to wdl script',required = True)
    parser_submit.add_argument('--inputs', type=str, help='Path to wdl inputs')
    parser_submit.add_argument('--deps', type=str, help='Path to zipped dependencies file')
    parser_submit.add_argument('--label', type=str, help='Label of the workflow',default = '')
    parser_submit.add_argument('--monitor',type=str,default="gs://fg-analysis-public-resources/monitor_script.sh",help="give custom monitoring script path in cloud")
    parser_submit.add_argument('--disable-monitoring',action="store_true",help='Disable task monitoring')

    label_options = parser_submit.add_mutually_exclusive_group(required=True)
    label_options.add_argument('--options', type=str, help='Workflow option json')
    label_options.add_argument('--google_labels', '--l', type=str, help='Labels (comma separated key=value list) of the workflow for google. Must contain product at minimum.')
    # metadata parser
    parser_meta = subparsers.add_parser('meta', aliases = ['metadata'],help="Requests metadata and summaries of workflows")
    parser_meta.add_argument("id", nargs='?',type= str,help="workflow id",default = "")
    parser_meta.add_argument("--file", type=str  ,help="Use already downloaded meta json file as data")
    parser_meta.add_argument("--minkeys", action="store_true"  ,help="Print summary of workflow")
    parser_meta.add_argument("--no_calls", action="store_true"
            ,help="If don't get call level data. In this way failed jobs can be listed for a workflow with too many rows")
    parser_meta.add_argument("--summary",'-s', action="store_true"  ,help="Print summary of workflow")
    parser_meta.add_argument("--running",'-r', action="store_true"  ,help="Print whether it's running or not")
    parser_meta.add_argument("--failed_jobs", action="store_true"  ,help="Print summary of failed jobs after each workflow")
    parser_meta.add_argument("--summarize_failed_jobs", action="store_true"  ,help="Print summary of failed jobs over all workflow")
    parser_meta.add_argument("--print_jobs_with_status", type=str ,help="Print summary of jobs with specific status jobs")
    parser_meta.add_argument("--cromwell_timeout", type=int, default=60  ,help="Time in seconds to wait for response from cromwell")

    parser_out = subparsers.add_parser('outfiles', aliases = ['outfiles'],help="Prints out content of elems under ")
    parser_out.add_argument("id",type= str,help="workflow id")
    parser_out.add_argument("tag",type= str,help="what output tag to print id")
    # abort parser
    parser_abort = subparsers.add_parser('abort' )
    parser_abort.add_argument("id", type= str,help="workflow id")

    parser_abort = subparsers.add_parser('connect' )
    parser_abort.add_argument("server", type=str,help="Cromwell server name")

    parser_log = subparsers.add_parser('log', help='prints the log')
    parser_log.add_argument("--n", type= int,default =10,help="number of latest jobs to print")
    parser_log.add_argument("--kw", type= str,help="Search for keyword")

    args = parser.parse_args()
    args.workflow_log = os.path.join(rootPath,'workflows.log')

    if args.outpath:
        rootPath=args.outpath + "/"

    if args.command =='abort':
        abort(args.id, args.port)
        update_log(args,args.id,'Aborted')

    elif args.command in ['metadata',"meta"]:
        if not args.id: args.id = get_last_job()
        print(args.id)

        if args.running:
            status = get_status(args.id, port=args.port, timeout=args.cromwell_timeout,nocalls=args.no_calls, minkeys=args.minkeys,http_port=args.http_port)
            args.summary = args.failed_jobs = False
            update_log(args,args.id,status)
        if args.summary or args.failed_jobs:
            if args.file:
                metadat=json.load(open(args.file))
            else:
                metadat = get_metadata(args.id, port=args.port, timeout=args.cromwell_timeout,
                            nocalls=args.no_calls, minkeys=args.minkeys,http_port=args.http_port)
            status = metadat['status']
            update_log(args,args.id,status)
            top_call_counts, summary = print_summary(metadat, args=args, port=args.port ,
                            expand_subs=True, timeout=args.cromwell_timeout )
            callstat = "\n".join([ "Calls for " + stat + "... " + ",".join([ f'{call}:{n}' for call,n in calls.items()])  for stat,calls in top_call_counts.items()])
            print("Total call statuses across subcalls:")
            print(callstat)

            if args.summarize_failed_jobs:
                if not args.no_calls:
                    failures = []
                    for c, s in summary.items():
                        failures.extend(s['failed_jobs'])
                    print_failed_jobs(failures)

                if "failures" in metadat:
                    print("print top level failures")
                    print_top_level_failure(metadat)


    elif args.command == "submit":

        wf_opts = process_inputs(args)
        print(args.wdl,args.inputs,args.label,wf_opts)
        submit(wdlPath=args.wdl, inputPath=args.inputs,port=args.port,wf_opts = wf_opts,label=args.label,
        dependencies= args.deps, options=args.options, http_port=args.http_port)

    elif args.command == "connect":
        print("Trying to connect to server...")
        subprocess.check_call(f'gcloud compute ssh {args.server} -- -f -n -N -D localhost:{args.port} -o "ExitOnForwardFailure yes"',
                    shell=True, encoding="ASCII")
        print(f'Connection opened to {args.server} via localhost:{args.port}')

    elif args.command == "outfiles":
        metadat = get_metadata(args.id, port=args.port, timeout=60,
                    minkeys=True,http_port=args.http_port)
        tag = args.tag

        def printfiles(lst):

            for l in lst:
                if isinstance(l,list):
                    printfiles(l)
                else:
                    print(l)

        printfiles(metadat["outputs"][tag])



    if args.command == "log":
        with open(args.workflow_log,'rt') as i:
            data = [elem.strip() for elem in i.readlines()]
        if args.kw:
            data = [elem for elem in data if args.kw in elem]
        idx = min(args.n,len(data))
        for line in data[-idx:]: print(line)
