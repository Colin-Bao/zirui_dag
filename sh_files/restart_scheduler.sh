#!/bin/bash
conda activate zirui_env
kill $(cat /home/lianghua/rtt/soft/airflow/airflow-scheduler.pid)
ps -ef|egrep 'scheduler'|grep -v grep|awk '{print $2}'|xargs kill -9
nohup airflow scheduler > log/schedulerout.log 2>&1 &


