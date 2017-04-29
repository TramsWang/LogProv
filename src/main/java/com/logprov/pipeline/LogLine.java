package com.logprov.pipeline;

/**
 * @author  Ruoyu Wang
 * @version 1.3
 * Date:    2017.04.29
 *
 *   Log structure, containing information about the execution of one Pig operation.
 */
public class LogLine {

    /* Pipeline execution ID */
    public String pid;

    /* Name of input variable, separated by ',' if many */
    public String srcvar;

    /* Index of input variable, separated by ',' if many */
    public String srcidx;

    /* Name of operation */
    public String operation;

    /* Name of output variable, separated by ',' if many */
    public String dstvar;

    /* Index of output variable, separated by ',' if many */
    public String dstidx;

    /* Evaluation score of the operation */
    public double score;

    /* String indicating data type of each column in output data, separated by ',' if many. 'n' for number, 's' for
       string. E.g. 4 columns "n,s,s,n" */
    public String column_type;

    /* String indicating columns in output data to be used as evidence of confidence check, separated by ',' if many.
       E.g. "0,3,4" */
    public String inspected_columns;

    /* String indicating confidence value of each inspected columns, separated by ',' if many and each part contains
       both column index and a double value. E.g. "0:0.15,3:45,4:1e-24" */
    public String confidence;

    /* Operation starting time stamp */
    public String start;

    /* Operation finishing time stamp */
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
