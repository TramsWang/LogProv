This project is a provenance and trustworthiness system for big data analysis. It was initially 
a NICTA project started in Oct. 2015. For now, all codes involved are written as a DEMO prototype.

Some detailed parameters:

    1. Java version: Oracle JAVA 8
    2. Elasticsearch version: 5.1.1(With plugein: sql)
    3. Hadoop version: 2.7.2
    4. Pig version: 0.16


Main contactor:

    Trams Wang:     babyfish92@163.com
    Daniel W. Sun:	daniel.sun@nicta.com.au
    
To illustrate the usage of the system, here's a simple example case:

1. Sample source data:

    1.1 'Standars.csv'
    
        Windows,4096,512
        Linux,8192,256
        Mac,1024,128
        
    1.2 'Softwares.csv'
    
        Wechat,Windows,4096,512,3.3
        Chrome,Windows,2048,128,4.6
        Minesweeper,Windows,64,32,3.7
        Safari,Mac,1536,64,4.4
        iTunes,Mac,512,64,4.8
        Messages,Mac,512,256,4.1
        Firefox,Linux,4096,128,3.5
        Emacs,Linux,256,32,4.8
        LibreOffice,Linux,1024,512,4.2

2. Pig Latin script: 'Sample.pig':

        REGISTER    LogProvUDF.jar;
        DEFINE      InterStore com.logprov.pigUDF.InterStore('http://localhost:58888');
        
        standards = LOAD 'Standards.csv' USING com.logprov.pigUDF.ProvLoader('standards',
            'http://localhost:58888', 'StorePath', 'Remark info')
            AS (Platform:chararray, MaxMem:int, MinMem:int);
        softwares = LOAD 'Softwares.csv' USING com.logprov.pigUDF.ProvLoader('softwares',
            'http://localhost:58888', 'StorePath', 'Remark info')
            AS (Name:chararray, Platform:chararray, MaxMem:int, MinMem:int, Rate:float);
        
        join1 = FOREACH (JOIN standards BY Platform, softwares BY Platform)
            GENERATE FLATTEN(InterStore('standards,softwares', 'JOIN-1', 'join1', *))
            AS (standards::Platform:chararray, standards::MaxMem:int, standards::MinMem:int,
            softwares::Name:chararray, softwares::Platform:chararray, softwares::MaxMem:int,
            softwares::MinMem:int, softwares::Rate:float);
        qualified = FOREACH (FOREACH (FILTER join1 BY softwares::MaxMem <= standards::MaxMem AND
            softwares::MinMem <= standards::MinMem) GENERATE softwares::Name AS Name:chararray)
            GENERATE FLATTEN(InterStore('join1', 'QUALIFICATION', 'qualified', *))
            AS (Name:chararray);
        qualified_cnt = FOREACH (GROUP qualified ALL) GENERATE COUNT(qualified);
        STORE qualified_cnt INTO 'Qualified' USING PigStorage(',');
        
        liked = FOREACH (FILTER softwares BY Rate >= 4.0)
            GENERATE FLATTEN(InterStore('softwares', 'FILTER_LIKED', 'liked', *))
            AS (Name:chararray, Platform:chararray, MaxMem:int, MinMem:int, Rate:float);
        join2 = FOREACH (FOREACH (JOIN qualified BY Name, liked BY Name) GENERATE qualified::Name
            AS Name:chararray)
            GENERATE FLATTEN(InterStore('qualified,liked', 'JOIN-2', 'join2', *))
            AS (Name:chararray);
        join2_cnt = FOREACH (GROUP join2 ALL) GENERATE COUNT(join2);
        STORE join2_cnt INTO 'QualifiedAndLiked' USING PigStorage(',');
        
        disliked = FOREACH (FILTER softwares BY Rate < 4.0)
            GENERATE FLATTEN(InterStore('softwares', 'FILTER_DISLIKED', 'disliked', *))
            AS (Name:chararray, Platform:chararray, MaxMem:int, MinMem:int, Rate:float);
        join3 = FOREACH (FOREACH (JOIN qualified BY Name, disliked BY Name) GENERATE qualified::Name
            AS Name:chararray)
            GENERATE FLATTEN(InterStore('qualified,disliked', 'JOIN-3', 'join3', *))
            AS (Name:chararray);
        join3_cnt = FOREACH (GROUP join3 ALL) GENERATE COUNT(join3);
        STORE join3_cnt INTO 'QualifiedButNotLiked' USING PigStorage(',');

3. Execution shell script: 'Sample.sh':

        #!/bin/bash
        
        hdfs dfs -rm -r Qualified* pid.txt;
        pig Sample.pig;
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
        