package com.logprov.pigUDF;

import com.google.gson.Gson;
import com.logprov.Config;
import com.logprov.pipeline.LogLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author Ruoyu Wang
 * @version 3.0
 * Date: Apr. 05, 2017
 *
 *   Provenance storing function for storing intermediate results. Syntax goes below:
 *
 *   REGISTER InterStore com.logprov.pigUDF.InterStore('Pipeline_Monitor_URL');
 *   dstvar = FOREACH dstvar GENERATE FLATTEN(InterStore('srcvar', 'processor', 'dstvar', *));
 *
 *   Where, you should always initialize this function via Pig Latin macro definition, providing it with proper address
 * information about pipeline monitor. 'dstvar' is the relation that's going to be stored, we suggest using same relation name when applying this
 * operation for the sake of performance and correctness; 'srcvar' is a string denotes the source variable, if there's
 * more than one, all source variable names are separated by ','; 'processor' stands for the user specified name for
 * this procedure; 'dstvar' is the name of the variable user would like to store data from; last asterisk is compulsory
 * for it means the entire data content.
 *
 *   This function shall always be used inside a FLATTEN operator, as shown in the example below:
 *   E.g.
 *   REGISTER InterStore com.logprov.pigUDF.InterStore('http://localhost:8888');
 *   ...
 *   dstvar = JOIN srcvar1 BY $0, srcvar2 BY $0;
 *   dstvar = FOREACH dstvar GENERATE FLATTEN(InterStore('srcvar1,srcvar2', 'Example', 'dstvar', *));
 *   ...
 */
public class InterStore extends EvalFunc<Tuple>{

    private FileSystem hdfs;
    private PrintWriter file;
    private String ps_location;
    private String pid;
    private int lines;

    private InterStore(){}

    /* Put to public only initializer with parameters. */
    public InterStore(String pm_url) throws IOException
    {
        file = null;
        lines = -1;
        ps_location = pm_url;
        hdfs = null;
    }

    /**
     *   Main body of storing procedure. Store one record line at a time. Fields in 'input' should be:
     *   $0: 'srcvars'
     *   $1: 'processor'
     *   $2: 'dstvar'
     *   $3,...: record content
     *
     * @param input Source data
     * @return Stored tuple.
     * @throws IOException
     */
    public Tuple exec(Tuple input) throws IOException
    {
        if (null == file)
        {
            System.out.println("INTER:: Create");
            System.out.flush();
            String srcvar = (String)input.get(0) + '\n';
            String operation = (String)input.get(1) + '\n';
            String varname = (String)input.get(2);

            /* Get HDFS connection */
            String hd_conf_dir = System.getenv("HADOOP_CONF_DIR");
            if (null == hd_conf_dir)
                throw new IOException("INTER:: Environment variable 'HADOOP_CONF_DIR' not set!!");
            Configuration conf = new Configuration();
            conf.addResource(new Path(hd_conf_dir + "/core-site.xml"));
            conf.addResource(new Path(hd_conf_dir + "/hdfs-site.xml"));
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            System.out.println("INTER:: Connecting to: " + conf.get("fs.defaultFS"));
            System.out.flush();
            hdfs = FileSystem.get(conf);

            /* Open PID file */
            if (!hdfs.exists(new Path(Config.PID_FILE)))
                throw new IOException(String.format("INTER:: PID file: '%s' not found!", Config.PID_FILE));
            BufferedReader pidfile = new BufferedReader(new InputStreamReader(hdfs.open(new Path(Config.PID_FILE))));
            pid = pidfile.readLine() + '\n';
            pidfile.close();

            /* Send parameters and get storage path from PM */
            URL url = new URL(ps_location + Config.CONT_REQUIRE_DATA_STORAGE);
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("PUT");
            con.setDoInput(true);
            con.setDoOutput(true);
            OutputStream out = con.getOutputStream();
            out.write(pid.getBytes());
            out.write(srcvar.getBytes());
            out.write(operation.getBytes());
            out.write(varname.getBytes());
            con.getOutputStream().close();
            int respcode = con.getResponseCode();
            if (400 == respcode)
                throw new IOException("INTER:: PM PUT Error!");

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String storage_path = in.readLine();
            in.close();
            System.out.println("INTER::<<<PATH>>>"+storage_path);
            System.out.flush();

            /* Open output file */
            file = new PrintWriter(hdfs.create(new Path(storage_path)));
            //file = new PrintWriter(storage_path);
            lines = 0;
        }

        Tuple t = TupleFactory.getInstance().newTuple();
        int len = input.size();
        for (int i = 3; i < len; i++)
        {
            t.append(input.get(i));
        }
        file.println(t.toDelimitedString(","));
        file.flush();
        lines++;

        return t;
    }

    @Override
    protected void finalize() throws Throwable {
        file.close();
        hdfs.close();
    }
}
