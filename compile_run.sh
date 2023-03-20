export HADOOP_CLASSPATH=$(hadoop classpath)
hadoop fs -rm -r /count_connected_components/output
rm -r compiled
mkdir compiled
javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'CountConnectedComponents.java'
jar -cvf compiledCountConnectedComponents.jar -C compiled/ .
hadoop jar compiledCountConnectedComponents.jar CountConnectedComponents /count_connected_components/input /count_connected_components/output
hadoop dfs -cat /count_connected_components/output/*