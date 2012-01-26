#!/bin/bash

VIRTUALENV_INSTALLED=`which virtualenv`
if [ "$VIRTUALENV_INSTALLED" == "" ]; then
    echo -e "virtualenv NOT found."
    exit 1
fi

virtualenv --no-site-packages virtualenv
source virtualenv/bin/activate
pip install -r requirements.txt

export PYTHONPATH=.:$PYTHONPATH
echo "Run \"source virtualenv/bin/activate\" to activate this virtualenv."

