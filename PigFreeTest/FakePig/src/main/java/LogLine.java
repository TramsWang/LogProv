/**
 * Created by trams on 12/04/17.
 */
public class LogLine {
    public String pid;
    public String srcvar;
    public String srcidx;
    public String operation;
    public String dstvar;
    public String dstidx;
    public double score;
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
        start = null;
        finish = null;
    }
}
