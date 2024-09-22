#! /usr/bin/env bash


hive -f user.sql
hive -f ip.sql
hive -f subnet.sql
hive -f log.sql