# hadoop-stuffs
Practice using Hadoop
/home/bqhung/home/repo/hadoop-stuffs/compile_run.sh




## Start service

sudo service ssh restart

cd ~/hadoop/hadoop-3.3.2

sbin/start-dfs.sh

sbin/start-yarn.sh

## End service

cd ~/hadoop/hadoop-3.3.2

sbin/stop-dfs.sh

sbin/stop-yarn.sh

DFS: [http://localhost:9870/dfshealth.html#tab-overview](http://localhost:9870/dfshealth.html#tab-overview)

YARN web portal: [http://localhost:8088/cluster](http://localhost:8088/cluster)

## Running script 1

hadoop fs -mkdir /WordCountTutorial

hadoop fs -mkdir /WordCountTutorial/Input

Change directory into workspace directory

cd ~

cd nerd

hadoop 

hadoop fs -put Input.txt /WordCountTutorial/Input

mkdir compiled

export HADOOP_CLASSPATH=$(hadoop classpath)

javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'WordCount.java’

jar -cvf compiledWordCount.jar -C compiled/ .

hadoop jar compiledWordCount.jar WordCount /WordCountTutorial/Input /WordCountTutorial/Output

hadoop dfs -cat /WordCountTutorial/Output/*

[https://youtu.be/6sK3LDY7Pp4](https://youtu.be/6sK3LDY7Pp4)

[https://kontext.tech/article/978/install-hadoop-332-in-wsl-on-windows](https://kontext.tech/article/978/install-hadoop-332-in-wsl-on-windows) 

## Running script 2

!cd ~/nerd

!mkdir unhealthy_relationship

!mkdir unhealthy_relationship/src

!cd unhealthy_relationship/src

!mkdir compiled

create Unhealthy_relationship.java

create Input.txt

hadoop fs -mkdir /unhealthy_relationship

hadoop fs -mkdir /unhealthy_relationship/input

hadoop fs -put input.txt /unhealthy_relationship/input

Go to the [localhost](http://localhost) of dfs to check  the directory

export HADOOP_CLASSPATH=$(hadoop classpath)

javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'Unhealthy_relationship.java’

jar -cvf compiledUnhealthyRelationship.jar -C compiled/ .

hadoop jar compiledUnhealthyRelationship.jar Unhealthy_relationship /unhealthy_relationship/input /unhealthy_relationship/output

hadoop dfs -cat /unhealthy_relationship/output/*

## Utility

/home/bqhung/home/repo/hadoop-stuffs/compile_run.sh

hadoop fs -rm -r /unhealthy_relationship/output

rm -r compiled

mkdir compiled

visit dfs managment (localhost:9870), logs, userlogs: [http://localhost:9870/dfshealth.html#tab-overview](http://localhost:9870/dfshealth.html#tab-overview)

choose the latest application, latest container, checkout the stderr

first container: nothing

second container: mapper’s log

third container: reducer’s log






## Start service

sudo service ssh restart

cd ~/hadoop/hadoop-3.3.2

sbin/start-dfs.sh

sbin/start-yarn.sh

## End service

cd ~/hadoop/hadoop-3.3.2

sbin/stop-dfs.sh

sbin/stop-yarn.sh

DFS: http://localhost:9870/dfshealth.html#tab-overview

YARN web portal: http://localhost:8088/cluster

## Running script 1

hadoop fs -mkdir /WordCountTutorial

hadoop fs -mkdir /WordCountTutorial/Input

Change directory into workspace directory

cd ~

cd nerd

hadoop 

hadoop fs -put Input.txt /WordCountTutorial/Input

mkdir compiled

export HADOOP_CLASSPATH=$(hadoop classpath)

javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'WordCount.java’

jar -cvf compiledWordCount.jar -C compiled/ .

hadoop jar compiledWordCount.jar WordCount /WordCountTutorial/Input /WordCountTutorial/Output

hadoop dfs -cat /WordCountTutorial/Output/*

https://youtu.be/6sK3LDY7Pp4

https://kontext.tech/article/978/install-hadoop-332-in-wsl-on-windows 

## Running script 2

!cd ~/nerd

!mkdir unhealthy_relationship

!mkdir unhealthy_relationship/src

!cd unhealthy_relationship/src

!mkdir compiled

create Unhealthy_relationship.java

create Input.txt

hadoop fs -mkdir -p /unhealthy_relationship/input

hadoop fs -put input.txt /unhealthy_relationship/input

Go to the [localhost](http://localhost) of dfs to check  the directory

export HADOOP_CLASSPATH=$(hadoop classpath)

javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'Unhealthy_relationship.java’

jar -cvf compiledUnhealthyRelationship.jar -C compiled/ .

hadoop jar compiledUnhealthyRelationship.jar Unhealthy_relationship /unhealthy_relationship/input /unhealthy_relationship/output

hadoop dfs -cat /unhealthy_relationship/output/*

## Utility

/home/bqhung/home/repo/hadoop-stuffs/compile_run.sh

hadoop fs -rm -r /unhealthy_relationship/output

rm -r compiled

mkdir compiled

visit dfs managment (localhost:9870), logs, userlogs: http://localhost:9870/dfshealth.html#tab-overview

choose the latest application, latest container, checkout the stderr

first container: nothing

second container: mapper’s log

third container: reducer’s log