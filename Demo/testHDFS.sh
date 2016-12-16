#!/bin/bash

STEP=5;
START=25;
END=25;
REPEAT=1;
PIGLATIN="RankingHDFS.pig";
LOGFILE=pigoutput.log;
TIMEFILE=time.log;

echo START: from $START to $END, stepped $STEP, repeated $REPEAT
echo Start > $TIMEFILE;

for i in $(seq $START $STEP $END)
do
    echo Test: $i
    echo Test: $i >> $TIMEFILE;
    for j in $(seq 1 $REPEAT)
    do
	hdfs dfs -rm -r timed_ordered density_ordered access_ordered HadoopTmp/*
	PID=$(curl 'localhost:58888/_start' -d 'HadoopTmp');
	echo New PID: $PID;
	sed -i 's/WifiStatusLarge_.*[.]csv/WifiStatusLarge_'$i'.csv/g' $PIGLATIN;
	sed -i "s/[,].*['].*-.*-.*[']/,'"$PID"'/g" $PIGLATIN;
	#pig $PIGLATIN &> $LOGFILE;
	#pig $PIGLATIN;
	#cat $LOGFILE | grep '(.*ms)' >> $TIMEFILE;

	#pig -x local $PIGLATIN &> $LOGFILE;
	#cat $LOGFILE | grep '(.*ms)';
	pig -x local $PIGLATIN;
	
	curl 'localhost:58888/_terminate' -d \'$PID\';
    done
done
