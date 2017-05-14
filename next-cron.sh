#!/bin/bash

java -cp target/daily-top-service-*.jar -Dloader.main=util.NextCron org.springframework.boot.loader.PropertiesLauncher $*
