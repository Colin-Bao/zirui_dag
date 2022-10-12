#!/bin/bash

nohup airflow webserver --port 8081 > log/webserverout.log 2>&1 &
nohup airflow scheduler > log/schedulerout.log 2>&1 &


