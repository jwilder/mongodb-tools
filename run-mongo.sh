#!/bin/bash
MONGODB_INSTALLED=`which mongod`
if [ "MONGODB_INSTALLED" == "" ]; then
    echo -e "mongodb NOT found."
    exit 1
fi

mkdir -p ./.data
mongod --dbpath ./.data