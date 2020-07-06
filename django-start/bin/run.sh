#!/bin/bash
source /etc/profile &> /dev/null
source ~/.bash_profile &> /dev/null

script_dir=$(cd "$(dirname "$0")"; pwd)
cd ${script_dir}
base_dir=$(pwd)/..
echo "base_dir: $base_dir"

echo "clean old files"
rm -rf source
rm -rf work
mkdir source
mkdir work

echo "git pull and mvn build"
cd source
git clone https://code.aliyun.com/middleware-contest-2020/django-java.git
cd django-java

mvn clean install -DskipTests=true

echo "uncompress and run"
cd ../../work
cp ../source/django-java/django-start/target/*.tar.gz .
tar -zxvf *.tar.gz
cd $(ls -d */ | grep django-start)

echo "django will start"
sh ./bin/bin.sh start
echo "django was start"