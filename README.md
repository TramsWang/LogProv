This project is a provenance and trustworthiness system for big data analysis. It was initially 
a NICTA project started in Oct. 2015. For now, all codes involved are written as a DEMO prototype.

Some detailed parameters:

    1. Java version: Oracle JAVA 8
    2. Elasticsearch version: 5.1.1(With plugein: sql)
    3. Hadoop version: 2.7.2
    4. Pig version: 0.16


Main contactor:

    Trams Wang:     babyfish92@163.com
    Daniel W. Sun:	daniel.sun@data61.csiro.au
    
To illustrate the usage of the system, here are two simple example case:

To run sample tests, two packages should firstly been generated, with and without dependencies. Suppose the one with 
dependencies is named `LogProv.jar` and the other `LogProvUDF.jar`. Then initiate Hadoop and elastic cluster. Then 
start the 'Simple Oracle'(server written as a sub-project in `PigFreeTest/SimpleOracle`). Next, start pipeline monitor 
in `LogProv.jar`. Finally, run samples.

1. Sample 1:

    This example shows one benchmark, examining whether a software is qualified. `Standards.csv` describes the benchmark 
    standards, scheme of which is `(platform:chararray, maximum_memory_limit:int, minimum_memory_limit:int)`. 
    `Softwares.csv` lists 9 software in 3 platforms(falsified) whose scheme is `(software:chararray, platform:chararry, 
    maximum_memory:int, minimum_memory:int, rank:float)`. This example uses these two files to collect all software 
    passed the benchmark as well as those passed and ranked higher(or lower) than 4.0. The script `Benchmark.sh` performs 
    the Pig script and fetches pipeline execution information.

    1.1. Sample source data:
    
    'Standars.csv'
    
        Windows,4096,512
        Linux,8192,256
        Mac,1024,128
        
    'Softwares.csv'
    
        Wechat,Windows,4096,512,3.3
        Chrome,Windows,2048,128,4.6
        Minesweeper,Windows,64,32,3.7
        Safari,Mac,1536,64,4.4
        iTunes,Mac,512,64,4.8
        Messages,Mac,512,256,4.1
        Firefox,Linux,4096,128,3.5
        Emacs,Linux,256,32,4.8
        LibreOffice,Linux,1024,512,4.2

    1.2. Pig Latin script:
    
    'Benchmark.pig':

        REGISTER    LogProvUDF.jar;
        DEFINE      InterStore com.logprov.pigUDF.InterStore('http://localhost:58888');
        
        standards = LOAD 'Standards.csv' USING com.logprov.pigUDF.ProvLoader('standards', '', '', 
            'http://localhost:58888', 'StorePath', 'Remark info')
            AS (Platform:chararray, MaxMem:int, MinMem:int);
        softwares = LOAD 'Softwares.csv' USING com.logprov.pigUDF.ProvLoader('softwares', '', '', 
            'http://localhost:58888', 'StorePath', 'Remark info')
            AS (Name:chararray, Platform:chararray, MaxMem:int, MinMem:int, Rate:float);
        
        join1 = FOREACH (JOIN standards BY Platform, softwares BY Platform)
            GENERATE FLATTEN(InterStore('standards,softwares', 'JOIN-1', 'join1', '', '',  *))
            AS (standards::Platform:chararray, standards::MaxMem:int, standards::MinMem:int,
            softwares::Name:chararray, softwares::Platform:chararray, softwares::MaxMem:int,
            softwares::MinMem:int, softwares::Rate:float);
        qualified = FOREACH (FOREACH (FILTER join1 BY softwares::MaxMem <= standards::MaxMem AND
            softwares::MinMem <= standards::MinMem) GENERATE softwares::Name AS Name:chararray)
            GENERATE FLATTEN(InterStore('join1', 'QUALIFICATION', 'qualified', '', '',  *))
            AS (Name:chararray);
        qualified_cnt = FOREACH (GROUP qualified ALL) GENERATE COUNT(qualified);
        STORE qualified_cnt INTO 'Qualified' USING PigStorage(',');
        
        liked = FOREACH (FILTER softwares BY Rate >= 4.0)
            GENERATE FLATTEN(InterStore('softwares', 'FILTER_LIKED', 'liked', '', '',  *))
            AS (Name:chararray, Platform:chararray, MaxMem:int, MinMem:int, Rate:float);
        join2 = FOREACH (FOREACH (JOIN qualified BY Name, liked BY Name) GENERATE qualified::Name
            AS Name:chararray)
            GENERATE FLATTEN(InterStore('qualified,liked', 'JOIN-2', 'join2', '', '',  *))
            AS (Name:chararray);
        join2_cnt = FOREACH (GROUP join2 ALL) GENERATE COUNT(join2);
        STORE join2_cnt INTO 'QualifiedAndLiked' USING PigStorage(',');
        
        disliked = FOREACH (FILTER softwares BY Rate < 4.0)
            GENERATE FLATTEN(InterStore('softwares', 'FILTER_DISLIKED', 'disliked', '', '',  *))
            AS (Name:chararray, Platform:chararray, MaxMem:int, MinMem:int, Rate:float);
        join3 = FOREACH (FOREACH (JOIN qualified BY Name, disliked BY Name) GENERATE qualified::Name
            AS Name:chararray)
            GENERATE FLATTEN(InterStore('qualified,disliked', 'JOIN-3', 'join3', '', '',  *))
            AS (Name:chararray);
        join3_cnt = FOREACH (GROUP join3 ALL) GENERATE COUNT(join3);
        STORE join3_cnt INTO 'QualifiedButNotLiked' USING PigStorage(',');

    1.3. Execution shell script:
    
    'Benchmark.sh':

        #!/bin/bash
        
        hdfs dfs -rm -r Qualified* pid.txt;
        pig Benchmark.pig;
        PID=`hdfs dfs -cat pid.txt`;
        curl localhost:58888/_terminate -d ${PID};
        curl localhost:58888/_eval -d $PID'
        join2
        http://localhost:59999';
        curl localhost:58888/_search -d 'data
        select * from logprov' > data.csv
        curl localhost:58888/_search -d 'meta
        select * from logprov' > meta.csv;
        curl localhost:58888/_search -d 'semantics
        select * from logprov' > semantics.csv;
        
2. Sample 2:

    This example illustrates the anomaly detection in LogProv. The three integers in `SampleData.csv` defines origin, 
    boundary and length of a random sequence. `Sample.pig` firstly generates a random sequence and then performs a 
    disturbing operation to the sequence. In `Sample.sh`, 10 'correct' history records are firstly generated. Then 
    another 'correct' pipeline is executed and tested. Finally an 'abnormal' pipeline is executed and tested. Different 
    confidence score will be listed in file `meta.csv`.

    2.1. Sample source data:
    
    'SampleData.csv':
    
        0,50,10
        
    2.2. Pig Latin script:
    
    'Sample.pig':
    
        REGISTER LogProvUDF.jar;
        
        DEFINE InterStore   com.logprov.pigUDF.InterStore('http://localhost:58888');
        DEFINE RandGen      com.logprov.sampleUDF.RandomGenerate();
        DEFINE Perturb      com.logprov.sampleUDF.Perturb('2.0');
        
        raw = LOAD 'SampleData.csv' USING com.logprov.pigUDF.ProvLoader('raw', 'n,n,n', '',
            'http://localhost:58888', 'InterMediateData', 'info')
            AS (origin:int, boundary:int, length:int);
        
        gen = FOREACH (FOREACH raw GENERATE FLATTEN(RandGen(*)))
            GENERATE FLATTEN(InterStore('raw', 'GENERATE', 'gen', 'n', '0', *))
            AS (int);
        
        result = FOREACH (FOREACH gen GENERATE Perturb(*))
            GENERATE FLATTEN(InterStore('gen', 'PERTURB', 'result', 'n', '0', *));
        
        STORE result INTO 'Result/Correct' USING PigStorage(',');
    
    2.3. Execute Shell script:
    
    'Sample.sh':
    
        #!/bin/bash
        
        PIGLATIN=Sample.pig
        SOURCEDATA=SampleData.csv
        INTERDATA_DIR=InterMediateData
        RESULTDATA_DIR=Result
        
        hdfs dfs -rm -r $INTERDATA_DIR $RESULTDATA_DIR $SOURCEDATA
        echo 0,50,10 > $SOURCEDATA
        hdfs dfs -put $SOURCEDATA
        
        # Generate History Data
        echo
        echo Generating history data...
        sed -i "s/sampleUDF.Perturb(.*)/sampleUDF.Perturb('0.0')/g" $PIGLATIN
        sed -i "s/'n', '.*', .*))/'n', '', *))/g"  $PIGLATIN
        for i in `seq 1 10`
        do
            hdfs dfs -rm -r $RESULTDATA_DIR pid.txt
            pig $PIGLATIN &> log
            cat log | grep "(*ms)"
            PID=`hdfs dfs -cat pid.txt`
            curl localhost:58888/_terminate -d ${PID}
        done
        
        # Inspect Into Correct Pipeline
        echo
        echo Performing correct pipeline...
        sed -i "s/sampleUDF.Perturb(.*)/sampleUDF.Perturb('0.0')/g" $PIGLATIN
        sed -i "s/'n', '.*', .*))/'n', '', *))/g"  $PIGLATIN
        hdfs dfs -rm -r $RESULTDATA_DIR pid.txt
        pig $PIGLATIN &> log
        cat log | grep "(*ms)"
        PID=`hdfs dfs -cat pid.txt`
        curl localhost:58888/_terminate -d ${PID}
        
        # Inspect Into Perturbed Pipeline
        echo
        echo Performing problematic pipeline...
        sed -i "s/sampleUDF.Perturb(.*)/sampleUDF.Perturb('2.0')/g" $PIGLATIN
        sed -i "s/'n', '.*', .*))/'n', '0', *))/g"  $PIGLATIN
        hdfs dfs -rm -r $RESULTDATA_DIR pid.txt
        pig $PIGLATIN &> log
        cat log | grep "(*ms)"
        PID=`hdfs dfs -cat pid.txt`
        curl localhost:58888/_terminate -d ${PID}
        curl localhost:58888/_eval -d $PID'
        result
        http://localhost:59999'
        
        # Query Info
        echo
        echo Querying information
        curl localhost:58888/_search -d 'data
        select * from logprov' > data.csv
        curl localhost:58888/_search -d 'meta
        select * from logprov' > meta.csv;
        curl localhost:58888/_search -d 'semantics
        select * from logprov' > semantics.csv;