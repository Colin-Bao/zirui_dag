# 关闭脚本stop_airflow.sh
#!/bin/bash
ps -ef|egrep 'scheduler|airflow-webserver'|grep -v grep|awk '{print $2}'|xargs kill -9
rm -rf ~/airflow/*.pid
echo "Stop success"

