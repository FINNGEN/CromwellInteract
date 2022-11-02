# Description

Simple script to interact with the cromwell server.

Suggestion: add to your `~./bashrc` the following line
`alias cromwell="python3 /PATH/TO/FILE/cromwell_interact.py"` making sure the path points to the file
and type `source ~/.bashrc`.
From now on you can simply invoke the script using the shortcut `cromwell`.

## Requirements
pyperclip:  `pip install pyperclip`
dateutil:   `pip install python-dateutil`
requests:   `pip install requests`

## Usage
The script is calling with the following syntax:
`cromwell [command]` where command is in the list `{submit,meta,metadata,outfiles,abort,connect,log} `


```
usage: cromwell_interact.py [-h] [--outpath OUTPATH] [--port PORT] [--http_port HTTP_PORT] {submit,meta,metadata,outfiles,abort,connect,log} ...

Run Cromwell commands from command line

positional arguments:
  {submit,meta,metadata,outfiles,abort,connect,log}
                        help for subcommand
    submit              submit a job
    meta (metadata)     Requests metadata and summaries of workflows
    outfiles (outfiles)
                        Prints out content of elems under
    log                 prints the log

optional arguments:
  -h, --help            show this help message and exit
  --outpath OUTPATH     Path to wdl script
  --port PORT           SSH port
  --http_port HTTP_PORT          Cromwell
```

## Examples

To further investigate usage for each command type :
`cromwell [command] --help`

### Connect

`cromwell connect [...] `
```
usage: cromwell_interact.py connect [-h] server

positional arguments:
  server      Cromwell server name

optional arguments:
  -h, --help  show this help message and exit

```
Use this script to connect to the cromwell server from which metadata is fetched.

### Submit
```
usage: cromwell_interact.py submit [-h] --wdl WDL [--inputs INPUTS] [--deps DEPS] [--label LABEL] (--options OPTIONS | --google_labels GOOGLE_LABELS)

optional arguments:
  -h, --help            show this help message and exit
  --wdl WDL             Path to wdl script
  --inputs INPUTS       Path to wdl inputs
  --deps DEPS           Path to zipped dependencies file
  --label LABEL         Label of the workflow
  --options OPTIONS     Workflow option json
  --google_labels GOOGLE_LABELS, --l GOOGLE_LABELS
                        Labels (comma separated key=value list) of the workflow for google. Must contain product at minimum.
```

This is the command used to submit jobs to the server.

An example would be `cromwell submit --wdl project.wdl --inputs project.json --options google_labels.json --label test` When a job is submitted, the info (date, wdl name, wld id, label) is appended at the bottom of a file called `workflows.log`.

`--inputs` does not need to be specified since the script will automatically look for a `.json` file with the same name as the `.wdl`, but it can be used to specify a separate input file.
`--label` is used to store in the `workflows.log` file (see `meta` command info)
`--deps` is used in case of subworklows

##### Labels

It's required to specify either `--options` or `--google_labels` as they allows us to monitor usage of resources based on each project.

`--options` is a json file where `google_label` is the main key followed by other subkyes, one of which *must* be `product`.
Here's a json template to use.
```
{
    "google_labels":{
        "project":"your-project",
        "product":"your-product"
    }
}
```
Similarly, one can pass the same information on the command line as comma separated key=value pairs:

`cromwell submit --wdl project.wdl  --options google_labels.json --label test --google_labels projects=your-project,product=your-product`


### Meta
```
usage: cromwell_interact.py meta [-h] [--file FILE] [--minkeys] [--no_calls] [--summary] [--running] [--failed_jobs] [--summarize_failed_jobs]
                                 [--print_jobs_with_status PRINT_JOBS_WITH_STATUS] [--cromwell_timeout CROMWELL_TIMEOUT]
                                 [id]

positional arguments:
  id                    workflow id

optional arguments:
  -h, --help            show this help message and exit
  --file FILE           Use already downloaded meta json file as data
  --minkeys             Print summary of workflow
  --no_calls            If don't get call level data. In this way failed jobs can be listed for a workflow with too many rows
  --summary, -s         Print summary of workflow
  --running, -r         Print whether it's running or not
  --failed_jobs         Print summary of failed jobs after each workflow
  --summarize_failed_jobs
                        Print summary of failed jobs over all workflow
  --print_jobs_with_status PRINT_JOBS_WITH_STATUS
                        Print summary of jobs with specific status jobs
  --cromwell_timeout CROMWELL_TIMEOUT
                        Time in seconds to wait for response from cromwell

```

`meta` will store a json file in the `tmp` subfolder where the script is found. The json summarizes (based on the request) the metadata info of the cromwell run.

The `id` input does not need to be specified as the last one is automatically fetched from the log file.

`cromwell meta -r` will produce the following output:
```
14bf0081-b5fa-4cf9-a9fd-086e772b94cc
curl -X GET "http://localhost:80/api/workflows/v1/14bf0081-b5fa-4cf9-a9fd-086e772b94cc/status" -H "accept: application/json" --socks5 localhost:5000  
{"status":"Succeeded","id":"14bf0081-b5fa-4cf9-a9fd-086e772b94cc"}CompletedProcess(args=['curl', '-X', 'GET', 'http://localhost:80/api/workflows/v1/14bf0081-b5fa-4cf9-a9fd-086e772b94cc/status', '-H', 'accept: application/json', '--socks5', 'localhost:5000'], returncode=0, stderr='')
```
which shows us that the job ran sucessfully.
In order to have a more structured breakdown one can use another flag:
`cromwell meta ccbb148d-ab1e-4881-97ed-86ae583e7f20 -s`
```
Metadata saved to /home/petekoti/Dropbox/Projects/CromwellInteract/tmp/ccbb148d-ab1e-4881-97ed-86ae583e7f20.json
Workflow name	ldsc_rg
Current status 	 Failed
Start	2021-10-01T14:47:50.648Z
End	2021-10-01T14:52:17.546Z

Call "ldsc_rg.filter_meta"
Basepath	gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-filter_meta
job statuses	 Done:1
Max time: 0.14 minutes, min time 0.14 minutes , average time 0.14 minutes
Max job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-filter_meta/stdout
Min job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-filter_meta/stdout

Call "ldsc_rg.munge_ldsc"
Basepath	gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-munge_ldsc/
job statuses	 Done:2
Max time: 0.29 minutes, min time 0.29 minutes , average time 0.29 minutes
Max job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-munge_ldsc/shard-1/stdout
Min job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-munge_ldsc/shard-0/stdout

Call "ldsc_rg.gather_h2"
Basepath	gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-gather_h2
job statuses	 Failed_Failed:1
Max time: 3.85 minutes, min time 3.85 minutes , average time 3.85 minutes
Max job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-gather_h2/stdout
Min job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-gather_h2/stdout

Total call statuses across subcalls:
Calls for ldsc_rg.filter_meta... Done:1
Calls for ldsc_rg.munge_ldsc... Done:2
Calls for ldsc_rg.gather_h2... Failed_Failed:1

```

This command will produce metadata for each task, showing where potentially each task failed. One can fetch more info about failed jobs with the `failed_jobs` flag:
`cromwell meta ccbb148d-ab1e-4881-97ed-86ae583e7f20 --failed_jobs`
```
ccbb148d-ab1e-4881-97ed-86ae583e7f20
curl -X GET "http://localhost:80/api/workflows/v1/ccbb148d-ab1e-4881-97ed-86ae583e7f20/metadata?expandSubWorkflows=false" -H "accept: application/json" --socks5 localhost:5000  
Metadata saved to /home/petekoti/Dropbox/Projects/CromwellInteract/tmp/ccbb148d-ab1e-4881-97ed-86ae583e7f20.json
Workflow name	ldsc_rg
Current status 	 Failed
Start	2021-10-01T14:47:50.648Z
End	2021-10-01T14:52:17.546Z

Call "ldsc_rg.filter_meta"
Basepath	gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-filter_meta
job statuses	 Done:1
Max time: 0.14 minutes, min time 0.14 minutes , average time 0.14 minutes
Max job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-filter_meta/stdout
Min job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-filter_meta/stdout
FAILED JOBS:
No failed jobs!


Call "ldsc_rg.munge_ldsc"
Basepath	gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-munge_ldsc/
job statuses	 Done:2
Max time: 0.29 minutes, min time 0.29 minutes , average time 0.29 minutes
Max job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-munge_ldsc/shard-1/stdout
Min job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-munge_ldsc/shard-0/stdout
FAILED JOBS:
No failed jobs!


Call "ldsc_rg.gather_h2"
Basepath	gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-gather_h2
job statuses	 Failed_Failed:1
Max time: 3.85 minutes, min time 3.85 minutes , average time 3.85 minutes
Max job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-gather_h2/stdout
Min job gs://fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-gather_h2/stdout
FAILED JOBS:
Failed	shard# -1
Task ldsc_rg.gather_h2:NA:1 failed. The job was stopped before the command finished. PAPI error code 9. Execution failed: generic::failed_precondition: while running "/cromwell_root/script": unexpected exit status 2 was not ignored
[UserAction] Unexpected exit status 2 while running "/cromwell_root/script": /cromwell_root/script: line 29: syntax error near unexpected token `('
/cromwell_root/script: line 29: `while read f; do echo $f; done < (cat /cromwell_root/fg-cromwell_fresh/ldsc_rg/ccbb148d-ab1e-4881-97ed-86ae583e7f20/call-gather_h2/write_lines_ef8cece4de0c3f7ddfe5ac466c5f26a3.tmp)'


Total call statuses across subcalls:
Calls for ldsc_rg.filter_meta... Done:1
Calls for ldsc_rg.munge_ldsc... Done:2
Calls for ldsc_rg.gather_h2... Failed_Failed:1
```

Thanks to the output one can be informed of how and where each task failed.

### Log
This command is a shortcut to navigate the `workflows.log` file that exists in the directory.
```
usage: cromwell_interact.py log [-h] [--n N] [--kw KW]

optional arguments:
  -h, --help  show this help message and exit
  --n N       number of latest jobs to print
  --kw KW     Search for keyword
```

`cromwell log` will return the last 10 jobs submitted as:
`DATE WDL_NAME WDL_ID LABEL`
```
2021-10-05 new 14cf9023-eb3c-49ad-8d34-03734c9b0fcb 1k
2021-10-05 new 1eda5cc3-ea04-4e71-8952-d01f41177f45 1k
2021-10-06 new d0b5590a-c2c2-499d-b4da-7e1c4dbe5679 full-run
2021-10-06 new b8961554-80be-49c1-9367-5255cddbfdd4 full-run
2021-10-06 new 6e8cbd31-9482-4e06-96c1-9926d333bb9e full-run
2021-10-06 new 948c7b3b-2cd3-4691-bc67-4e8b3c60bca0 full-run
2021-10-06 new 17922088-0884-442d-88d7-8db0c9e7ac9c full-run
2021-10-06 new 9186fbb9-47eb-46c5-9b80-19581e768cb3 full-run
2021-10-07 new 1afdb0e4-198b-4ff3-be09-27fdbd547db0
2021-10-07 new 14bf0081-b5fa-4cf9-a9fd-086e772b94cc test_files
```

One can change the number of lines with `--n`.

`--kw` allows to search for specific substrings in either the wdl name or label field. In this way one can fetch old specific jobs that share a pattern.

## Abort
Simple command to terminate a running wdl

```
usage: cromwell_interact.py abort [-h] id

positional arguments:
  id          workflow id

```

In this case the `id` argument is required as a safety measure.
