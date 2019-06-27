# Description

Simple script to interact with the cromwell server

## Requirements
pyperclip:  `pip install pyperclip`

## Usage

`python cromwell_interact.py [command] [arguments]`

### Commands

`submit` requires two arguments:\
`--wdl` : path to the .wdl file  \
`--inputs` : path to the .json file. If not passed, the script assumes the `.json` file has the same name as the wdl \
`--labels` : labels that will be logged \
The script then logs the date,id and labels to a `workflows.log` file in the script directory and it also automatically copies the id to the clipboard

`metadata` requires the workflow id as a positional argument

`abort` requires the workflow id as a positional argument