#!/usr/bin/env bash

source `which virtualenvwrapper.sh`
workon refresh-plex
pip install -r requirements.txt
python refresh_plex.py "$@"
deactivate
