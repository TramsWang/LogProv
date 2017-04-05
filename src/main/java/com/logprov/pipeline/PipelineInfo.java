package com.logprov.pipeline;

import java.util.Date;

/**
 * Created by:  Ruoyu Wang
 * Date:        2017.04.05
 *
 *   Pipeline information structure, containing sufficient fields to record a pipeline execution
 * history.
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
