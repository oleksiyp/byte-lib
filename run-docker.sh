#!/bin/bash

echo "Taking /data from current directory: $PWD/data"
echo "Taking /parsed from current directory: $PWD/parsed"
echo "Taking /root/.ssh from home directory: $HOME/.ssh"

docker run -v $PWD/data:/data -v $PWD/parsed:/parsed -v $HOME/.ssh:/root/.ssh top-wikipedia/daily-top-service
