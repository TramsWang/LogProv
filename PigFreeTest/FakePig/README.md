This sub-project is used for running quick tests without Pig running on Hadoop. The server used here simulates HTTP requests that will be sent by Pig. To configure the executing simulation, those following files should be correctly written:

  1. config.txt:

    This file should contain all file names of pipeline simulation configuration, where each line contains only one file name.

  2. \<single pipeline simulation configuration file\>:

    This file configures one specific pipeline simulation. The first two lines in the file are "HDFS storage root path" and "Remark information" respectively. You should use following commands to arrange the pipeline runtime emulating procedure.

      (1)"config:"

         This command may be followed by many lines specifying data and control flow of the pipeline, with the format:
         Srcvar;;Operation;;Dstvar;;<Specific data>

      (2)"eval:"

         This command may be followed by many lines specifying which variable to be evaluated. Each line here denotes one specific variable.

      (3)"term:"

         This command terminate the execution of the pipeline.

      (4)"search:"

         This command may be followed by many lines specifying what searching queries will be initiated, with the format:
         <Output file name>;;<Search Command>;;<Query line 1>;;...;;<Query line n>

An example of testing script is shown below:

  In file "config.txt":

    pipeline1.txt

  In file "pipeline1.txt"

    /HDFS/
    Basic test script
    config:
    unkown;;LOADER;;raw;;LOADER_DATA
    raw;;MEASURE;;measured;;MEASURE_DATA
    raw;;CLEANER;;cleaned;;CLEANER_DATA
    measured;;COUNTER;;count;;COUNTER_DATA
    cleaned;;LEFT;;ldata;;LEFT_DATA
    cleaned;;RIGHT;;rdata;;RIGHT_DATA
    count,ldata;;AGGREGATOR;;aggregated;;AGGREGATOR_DATA
    count,ldata;;COMBINE;;combined;;COMBINE_DATA
    ldata,rdata;;MATCH;;matched;;MATCH_DATA
    eval:
    matched
    term:
    eval:
    combined
    search:
    pipeline1_meta.csv;;meta;;select * from logprov
