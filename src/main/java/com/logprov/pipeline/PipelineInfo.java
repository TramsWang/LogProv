package com.logprov.pipeline;

import java.util.Date;

/**
 * @author  Ruoyu Wang
 * @version 1.0
 * Date:    2017.04.29
 *
 *   Pipeline information structure, containing sufficient fields to record a pipeline execution
 * history.
 */
public class PipelineInfo {

    /* Pipeline execution ID */
    public String pid;

    /* Root path of storing intermediate data and other files */
    public String hdfs_path;

    /* Remarks string */
    public String info;

    /* Pipeline starting timestamp */
    public String start;

    /* Pipeline finishing timestamp. Null indicating errors in the pipeline */
    public String finish;

    public PipelineInfo()
    {
        pid = null;
        hdfs_path = null;
        info = null;
        start = null;
        finish = null;
    }
}
