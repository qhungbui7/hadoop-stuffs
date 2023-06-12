export HADOOP_CLASSPATH=$(hadoop classpath)
# hadoop fs -rm -r /music_track/output
# rm -r compiled
# mkdir compiled
# javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'MusicTrack.java'
# jar -cvf compiledMusicTrack.jar -C compiled/ .
# hadoop jar compiledMusicTrack.jar MusicTrack /music_track/input /music_track/output
# hadoop dfs -cat /music_track/output/*

# hadoop fs -rm -r /patent/output
# rm -r compiled
# mkdir compiled
# javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'Patent.java'
# jar -cvf compiledPatent.jar -C compiled/ .
# hadoop jar compiledPatent.jar Patent /patent/input /patent/output
# hadoop dfs -cat /patent/output/*

# hadoop fs -rm -r /count_connected_components/output
# rm -r compiled
# mkdir compiled
# javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'CountConnectedComponents.java'
# jar -cvf compiledCountConnectedComponents.jar -C compiled/ .
# hadoop jar compiledCountConnectedComponents.jar CountConnectedComponents /count_connected_components/input /count_connected_components/output
# hadoop dfs -cat /count_connected_components/output/*



# hadoop fs -rm -r /WordCount/output
# rm -r compiled
# mkdir compiled
# javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'WordCount.java'
# jar -cvf compiledWordCount.jar -C compiled/ .
# hadoop jar compiledWordCount.jar WordCount /WordCount/input /WordCount/output
# hadoop dfs -cat /WordCount/output/*


hadoop fs -rm -r /element-wise_multiplication/output
rm -r compiled
mkdir compiled
javac -classpath ${HADOOP_CLASSPATH} -d 'compiled' 'ElementWiseMultiplication.java'
jar -cvf compiledElementWiseMultiplication.jar -C compiled/ .
hadoop jar compiledElementWiseMultiplication.jar ElementWiseMultiplication /element-wise_multiplication/input /element-wise_multiplication/output
hadoop dfs -cat /element-wise_multiplication/output/*