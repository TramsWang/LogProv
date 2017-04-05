package com.logprov.pipeline;

import java.util.Date;

/**
 * Created by babyfish on 16-10-26.
 */
public class PipelineInfo {
    public String pid;
    public String hdfs_path;
    public String info;
    public String start;
    public String finish;
    public String status;

    public PipelineInfo()
    {
        pid = null;
        hdfs_path = null;
        info = null;
        start = null;
        finish = null;
        status = null;
    }
}
