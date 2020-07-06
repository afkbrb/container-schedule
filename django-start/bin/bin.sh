#!/bin/bash
source /etc/profile &> /dev/null
source ~/.bash_profile &> /dev/null

script_dir=$(cd "$(dirname "$0")"; pwd)
cd ${script_dir}
base_dir=$(pwd)/..
echo "base_dir: $base_dir"

if [ ! -d ../logs ];then
    mkdir ../logs
fi

control=$1

jvmOpts="-server -Xms3G -Xmx3G"

case ${control} in
    "start")
       # nohup java ${jvmOpts} -cp "$base_dir/config:$base_dir/lib/*" com.tianchi.django.CalculateLauncher >>${base_dir}/logs/server.log 2>&1 &
       java ${jvmOpts} -cp "$base_dir/config:$base_dir/lib/*" com.tianchi.django.CalculateLauncher
    ;;
    "stop")
        ps aux | grep [j]ava | grep com.tianchi.django.CalculateLauncher | awk '{print $2}' | xargs -n 1 kill
    ;;
    *) echo "please input start|stop"
    ;;
esac