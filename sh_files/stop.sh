# 关闭脚本stop_airflow.sh
#!/bin/bash

ps -ef|egrep 'airflow-webserver'|grep -v grep|awk '{print $2}'|xargs kill -9
ps -ef|egrep 'scheduler'|grep -v grep|awk '{print $2}'|xargs kill -9
rm -rf ~/airflow/*.pid
echo "Stop success"

# kill -9 388890
# airflow webserver --port 8081
#sudo kill -9 $(lsof -i:8793 -t) 2> /dev/null
# P@ssw0rd
# sudo fuser - k 8793/tcp
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-webserver.pid)
# sudo lsof -i: 8793 | grep - v "PID" | awk '{print "kill -9",$2}' | sh
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-scheduler.pid)