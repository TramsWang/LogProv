package com.logprov.pipeline;

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
    public String column_type;  // 'n' for number, 's' for string. E.g. 4 columns "n,s,s,n"
    public String inspected_columns;  // Index of columns that should be checked. E.g. "0,3,4"
    public String confidence;
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
        column_type = null;
        inspected_columns = null;
        confidence = null;
        start = null;
        finish = null;
    }
}
