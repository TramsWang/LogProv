package com.logprov.pipeline;

/**
 * Created by:  Ruoyu Wang
 * Date:        2017.04.05
 *
 *   Elasticsearch response structure. Used for reverse from JSON string via Gson.
 */
public class ESResponse {

    public class Shards{
        public int total;
        public int successful;
        public int failed;
    }

    /* TODO: Is there any way of multi-inheritance to write these code more gracefuly? */
    public class Source{
        public String pid;
        public String srcvar;
        public String srcidx;
        public String operation;
        public String dstvar;
        public String dstidx;
        public String column_type;
        public String inspected_columns;
        public String distance;
        public double score;
        public String start;
        public String finish;

        //public String pid;
        public String hdfs_path;
        public String info;
        //public String start;
        //public String finish;
    }

    public class Hit{
        public String _index;
        public String _type;
        public String _id;
        public double _score;
        public Source _source;
    }

    public class Hits{
        public int total;
        public double max_score;
        public Hit[] hits;
    }

    public int took;
    public boolean timed_out;
    public Shards _shards;
    public Hits hits;
}
