#!/bin/bash

git remote update
if git diff --quiet remotes/origin/HEAD; then
  echo Nothing to update
else
  git pull && mvn clean package
fi
