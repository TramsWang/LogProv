import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Date;

/**
 * @author  Ruoyu Wang
 * @version 1.1
 * Date     2017.04.12
 */
public class Tester {

    public static final String CMD_CONFIG = "config:";
    public static final String CMD_EVALULATE = "eval:";
    public static final String CMD_TERMINATE = "term:";
    public static final String CMD_SEARCH = "search:";

    private static final int ST_START = 0;
    private static final int ST_CONFIG = 1;
    private static final int ST_EVALUATE = 2;
    private static final int ST_TERMINATE = 3;
    private static final int ST_SEARCH = 4;

    private BufferedReader in;
    private int state;
    private boolean running;
    private int linenum;
    private String pid;
    private Thread t;
    private FileSystem hdfs;
    private String pmlocation;
    private String oraclelocation;
    private Gson gson;

    private Tester(){}

    public Tester (String filename) throws IOException, InterruptedException
    {
        in = new BufferedReader(new FileReader(filename));
        state = ST_START;
        running = true;
        linenum = 0;
        t = new Thread();
        pmlocation = in.readLine();
        oraclelocation = in.readLine();
        gson = new Gson();

        String hd_conf_dir = System.getenv("HADOOP_CONF_DIR");
        hd_conf_dir = "/home/trams/hadoop-2.7.2";  //for debugging only
        if (null == hd_conf_dir)
            throw new IOException("Environment variable 'HADOOP_CONF_DIR' not set!!");
        Configuration conf = new Configuration();
        conf.addResource(new Path(hd_conf_dir + "/core-site.xml"));
        conf.addResource(new Path(hd_conf_dir + "/hdfs-site.xml"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        //hdfs = FileSystem.get(URI.create("hdfs://master:8020/"), conf);
        hdfs = FileSystem.get(conf);
        System.out.println(String.format("Connecting to: %s<should be: %s>", hdfs.getName(), conf.get("fs.defaultFS")));

        System.out.println(String.format("TESTER: '%s' initiated.", filename));

        String hdfs_path = in.readLine();
        String remarks = in.readLine();
        pid = simpleHttpRequest(pmlocation + "/_start", "GET", String.format("%s\n%s", hdfs_path, remarks));
        System.out.println("--------------------STARTED--------------------------\n");
    }

    public void test() throws IOException, InterruptedException
    {
        String line;
        while (running && (null != (line = in.readLine())))
        {
            linenum++;
            if (parseCommand(line)) continue;

            switch (state)
            {
                case ST_START:
                {
                    if (!"".equals(line))
                    {
                        System.err.println(String.format("Missing command at <%d>, halt.", linenum));
                        running = false;
                    }
                    break;
                }
                case ST_CONFIG:
                {
                    if (!"".equals(line))
                    {
                        String paras[] = line.split(";;");  //srcvar;;srcidx;;operation;;dstvar;;dstidx;;data
                        if (6 < paras.length)
                        {
                            System.err.println(String.format("Too many paras in configuration line<%d>, halt", linenum));
                            running = false;
                            break;
                        }
                        else if (6 > paras.length)
                        {
                            System.err.println(String.format("Too few paras in configuration line<%d>, halt", linenum));
                            running = false;
                            break;
                        }
                        LogLine log = new LogLine();
                        log.pid = pid;
                        log.srcvar = "".equals(paras[0])?null:paras[0];
                        log.srcidx = "".equals(paras[1])?null:paras[1];
                        log.operation = paras[2];
                        log.dstvar = "".equals(paras[3])?null:paras[3];
                        log.dstidx = "".equals(paras[4])?null:paras[4];
                        log.start = new Date().toString();
                        String path = simpleHttpRequest(pmlocation + "/_reqDS", "GET", gson.toJson(log));

                        /* Write to HDFS */
                        System.out.println("Write to:: " + path);
                        PrintWriter outfile = new PrintWriter(hdfs.create(new Path(path)));
                        outfile.println(paras[5]);
                        outfile.close();
                    }
                    break;
                }
                case ST_EVALUATE:
                {
                    simpleHttpRequest(pmlocation + "/_eval", "GET",
                            String.format("%s\n%s\n%s", pid, line, oraclelocation));
                    t.sleep(2000);
                    break;
                }
                case ST_TERMINATE:
                {
                    if (!"".equals(line))
                    {
                        System.err.println(String.format("Missing command at <%d>, halt.", linenum));
                        running = false;
                    }
                    break;
                }
                case ST_SEARCH:
                {
                    String paras[] = line.split(";;");  //outputfile;;line1;;line2;;...;;linen
                    PrintWriter file_out = new PrintWriter(paras[0]);
                    String sql = "";
                    for (int i = 1; i < paras.length; i++)
                    {
                        sql += paras[i] + '\n';
                    }
                    simpleHttpRequest(pmlocation + "/_search", "GET", sql, file_out);
                    file_out.close();
                    break;
                }
                default:
                    System.err.println(String.format("Unknown state: '%d', halt", state));
                    return;
            }
        }
        in.close();
        hdfs.close();
    }

    private boolean parseCommand(String s) throws InterruptedException, IOException
    {
        if (CMD_CONFIG.equals(s))
        {
            System.out.println("--------------------CONFIG---------------------");
            t.sleep(2000);
            state = ST_CONFIG;
            return true;
        }
        else if (CMD_EVALULATE.equals(s))
        {
            System.out.println("--------------------EVALUATE---------------------");
            t.sleep(2000);
            state = ST_EVALUATE;
            return true;
        }
        else if (CMD_TERMINATE.equals(s))
        {
            System.out.println("--------------------TERMINATE---------------------");
            t.sleep(2000);
            simpleHttpRequest(pmlocation + "/_terminate", "GET", pid);
            state = ST_TERMINATE;
            return true;
        }
        else if (CMD_SEARCH.equals(s))
        {
            System.out.println("--------------------SEARCH---------------------");
            t.sleep(2000);
            state = ST_SEARCH;
            return true;
        }
        return false;
    }

    public String simpleHttpRequest(String str_url, String method, String content) throws IOException
    {
        URL url = new URL(str_url);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(method);
        con.setDoInput(true);
        if (null == content)
        {
            con.setDoOutput(false);
        }
        else
        {
            con.setDoOutput(true);
            OutputStream out = con.getOutputStream();
            out.write(content.getBytes());
            out.close();
        }

        int resp_code = con.getResponseCode();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputline;
        String resp = "";
        while (null != (inputline = in.readLine()))
        {
            resp += inputline;
        }
        System.out.println(String.format("Response Code: %d", resp_code));
        System.out.println(String.format("Response: %s", resp));
        return resp;
    }

    public void simpleHttpRequest(String str_url, String method, String content, PrintWriter file) throws IOException
    {
        URL url = new URL(str_url);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(method);
        con.setDoInput(true);
        if (null == content)
        {
            con.setDoOutput(false);
        }
        else
        {
            con.setDoOutput(true);
            OutputStream out = con.getOutputStream();
            out.write(content.getBytes());
            out.close();
        }

        int resp_code = con.getResponseCode();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputline;
        while (null != (inputline = in.readLine()))
        {
            file.println(inputline);
        }
        System.out.println(String.format("Response Code: %d", resp_code));
    }
}
