#!/usr/bin/env bash

source `which virtualenvwrapper.sh`
workon refresh-plex
python refresh_plex
deactivate