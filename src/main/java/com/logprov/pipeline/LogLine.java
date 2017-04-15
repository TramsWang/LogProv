package com.logprov.pipeline;

import java.util.Date;

/**
 * Created by:  Ruoyu Wang
 * Date:        2017.04.05
 *
 *   Log structure, containing information about the execution of one Pig operation.
 */
public class LogLine {
    public String pid;
    public String srcvar;
    public String srcidx;
    public String operation;
    public String dstvar;
    public String dstidx;
    public double score;
    public String distance;
    public String start;
    public String finish;

    public LogLine()
    {
        pid = null;
        srcvar = null;
        srcidx = null;
        operation = null;
        dstvar = null;
        dstidx = null;
        score = 0.0;
        distance = null;
        start = null;
        finish = null;
    }
}
