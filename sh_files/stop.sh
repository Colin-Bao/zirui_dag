# 关闭脚本stop_airflow.sh
#!/bin/bash

ps -ef|egrep 'airflow-webserver'|grep -v grep|awk '{print $2}'|xargs kill -9
sudo kill -9 $(lsof -i:8081 -t) 2> /dev/null
sudo kill $(cat /home/lianghua/rtt/soft/airflow/airflow-webserver.pid)

ps -ef|egrep 'scheduler'|grep -v grep|awk '{print $2}'|xargs kill -9
kill -9 $(lsof -i:8793 -t) 2> /dev/null
kill $(cat /home/lianghua/rtt/soft/airflow/airflow-scheduler.pid)


rm -rf ~/airflow/*.pid
echo "Stop success"

# kill -9 388890
# airflow webserver --port 8081
#sudo kill -9 $(lsof -i:8793 -t) 2> /dev/null
# P@ssw0rd
# sudo fuser - k 8793/tcp
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-webserver.pid)
# sudo lsof -i: 8793 | grep - v "PID" | awk '{print "kill -9",$2}' | sh
