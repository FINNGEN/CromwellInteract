# Description

Simple script to interact with the cromwell server

## Requirements
pyperclip --. `pip install pyperclip`

## Usage

`python cromwell_interact.py [command] [arguments]`

### Commands

`submit` requires two arguments:\
`--wdl` : path to the .wdl file  \
`--inputs` : path to the .json file

it also automatically copies the id to the clipboard

`metadata` requires:\
`--id`: the worflow id
