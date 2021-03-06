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
import java.util.Date;

/**
 * @author  Ruoyu Wang
 * @version 2.1
 * Date:    2017.04.29
 *
 *   LogProv storing function for storing intermediate results.
 *
 * Syntax:
 *   DEFINE InterStore com.logprov.pigUDF.InterStore('[pipeline monitor URL]');
 *   [dstvar] = FOREACH [dstvar] GENERATE FLATTEN(InterStore('[srcvar]', '[operation]', '[dstvar]', '[column type]',
 * '[inspected columns]', *));
 *
 *   [dstvar] is the name for output variable who receives data from loader; [srcvar] is the name for input variable;
 * [operation] is the name of this step of operation; [column type] is a string indicating the type of each column in
 * data, where 'n' and 's' stands for numerical and textual data respectively, separated by ',' if many; [inspected
 * columns] is a string indicating the indices of columns used as evidences to discover operation malfunction, ','
 * separated if many.
 *   This function should always be initialized via Pig Latin macro definition, provided with proper URL of pipeline
 * monitor server. Moreover, this function shall always be used inside a FLATTEN operator since it returns each record
 * wrapped as a Pig tuple.
 *
 * Example 1:
 *   DEFINE InterStore com.logprov.pigUDF.InterStore('http://localhost:58888');
 *   ...
 *   dstvar = JOIN srcvar1 BY $0, srcvar2 BY $0;
 *   dstvar = FOREACH dstvar GENERATE FLATTEN(InterStore('srcvar1,srcvar2', 'Example', 'dstvar', *));
 *   ...
 *
 * Example 2:
 *   DEFINE InterStore com.logprov.pigUDF.InterStore('http://localhost:58888');
 *   ...
 *   dstvar = FOREACH (JOIN srcvar1 BY $0, srcvar2 BY $0) GENERATE FLATTEN(InterStore('srcvar1,srcvar2', 'Example',
 * 'dstvar', *));
 *   ...
 */
public class InterStore extends EvalFunc<Tuple>{

    private FileSystem hdfs;
    private PrintWriter file;
    private String ps_location;
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
     *   $3: 'column types'
     *   $4: 'inspected columns'
     *   $5,...: record content
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
            LogLine log = new LogLine();
            log.srcvar = (String)input.get(0);
            log.operation = (String)input.get(1);
            log.dstvar = (String)input.get(2);
            log.column_type = (String)input.get(3);
            log.inspected_columns = (String)input.get(4);
            log.start = new Date().toString();

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
            log.pid = pidfile.readLine();
            pidfile.close();

            /* Send parameters and get storage path from PM */
            URL url = new URL(ps_location + Config.CONT_REQUIRE_DATA_STORAGE);
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("PUT");
            con.setDoInput(true);
            con.setDoOutput(true);
            OutputStream out = con.getOutputStream();
            out.write(new Gson().toJson(log).getBytes());
            out.close();
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
        for (int i = 5; i < len; i++)
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
