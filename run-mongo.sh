#!/bin/bash
MONGODB_INSTALLED=`which mongod`
if [ "MONGODB_INSTALLED" == "" ]; then
    echo -e "mongodb NOT found."
    exit 1
fi

${PORT:=27017}

mkdir -p ./.data
mongod --dbpath ./.data --port $PORT
