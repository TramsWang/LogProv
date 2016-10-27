package com.logprov.pipeline;

import com.google.gson.Gson;
import com.logprov.Config;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

/**
 * Created by babyfish on 16-10-26.
 */
public class PipelineMonitor {

    private class VarAllocInfo{
        public String index;
        public int alloc_num;

        private VarAllocInfo(){}

        public VarAllocInfo(String idx)
        {
            index = idx;
            alloc_num = 0;
        }
    }

    private class PipelineRecord{
        String hdfs_path;
        HashMap<String, VarAllocInfo> vars;

        private PipelineRecord(){}

        public PipelineRecord(String hdfs_path)
        {
            this.hdfs_path = hdfs_path;
            vars = new HashMap<String, VarAllocInfo>();
        }
    }

    private class Start implements HttpHandler {

        public void handle(HttpExchange t)
        {
            try
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));

                /* Generate Pipeline Information */
                PipelineInfo pinfo = new PipelineInfo();
                pinfo.pid = UUID.randomUUID().toString();
                pinfo.hdfs_path = in.readLine();
                pinfo.info = in.readLine();
                pinfo.start = new Date().toString();
                in.close();

                /* Send PID back to Pig */
                t.sendResponseHeaders(200, pinfo.pid.getBytes().length);
                OutputStream out = t.getResponseBody();
                out.write(pinfo.pid.getBytes());
                out.close();

                /* Record pipeline info in hash map */
                pipelines.put(pinfo.pid, new PipelineRecord(pinfo.hdfs_path));

                /* Write Pipeline info into ESS */
                es_slave_pipelines.send(gson.toJson(pinfo));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private class ReqDS implements HttpHandler {

        public void handle(HttpExchange t)
        {
            try
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
                LogLine log = new LogLine();
                log.pid = in.readLine();
                log.srcvar = in.readLine();
                log.operation = in.readLine();
                log.dstvar = in.readLine();
                in.close();

                /* Check validity */
                PipelineRecord precord = pipelines.get(log.pid);
                if (null == precord)
                {
                    in.close();
                    String msg = "Invalid PID: No such pipeline running currently.";
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }

                /* Check allocate new entry or use old one */
                VarAllocInfo vinfo = precord.vars.get(log.dstvar);
                String storage_path = precord.hdfs_path;
                if (null == vinfo)
                {
                    /* Generate LogLine information */
                    log.dstidx = UUID.randomUUID().toString();
                    log.start = new Date().toString();

                    /* Record allocation mapping */
                    vinfo = new VarAllocInfo(log.dstidx);
                    precord.vars.put(log.dstvar, vinfo);

                    /* Return storage path */
                    storage_path += String.format("/%s/%s_%s/%s", log.pid, log.dstidx, log.dstvar,
                            "part_" + Integer.toString(vinfo.alloc_num));
                    vinfo.alloc_num++;
                    t.sendResponseHeaders(200, storage_path.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(storage_path.getBytes());
                    out.close();

                    /* Complete LogLine(srcidx) and write to ESS */
                    String srcvars[] = log.srcvar.split(",");
                    boolean first = true;
                    for (String tmpsrcvar : srcvars)
                    {
                        VarAllocInfo tmpvinfo = precord.vars.get(tmpsrcvar);
                        if (first)
                        {
                            log.srcidx = (null == tmpvinfo)?"LOAD":tmpvinfo.index;
                            first = false;
                        }
                        else
                        {
                            log.srcidx += ',' + ((null == tmpvinfo) ? "LOAD" : tmpvinfo.index);
                        }
                        es_slave_logs.send(gson.toJson(log));
                    }

                    VarAllocInfo tmpvinfo = precord.vars.get(log.srcvar);
                    log.srcidx = (null == tmpvinfo)?"LOAD":tmpvinfo.index;
                    es_slave_logs.send(gson.toJson(log));
                }
                else
                {
                    /* Return storage path */
                    storage_path += String.format("/%s/%s_%s/%s", log.pid, vinfo.index, log.dstvar,
                            "part_" + Integer.toString(vinfo.alloc_num));
                    vinfo.alloc_num++;
                    t.sendResponseHeaders(200, storage_path.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(storage_path.getBytes());
                    out.close();
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private class Terminate implements  HttpHandler{

        public void handle(HttpExchange t)
        {
            try
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
                String pid = in.readLine();
                in.close();

                /* Check validity */
                PipelineRecord precord = pipelines.get(pid);
                if (null == pid)
                {
                    String msg = "Invalid PID: No such pipeline is running currently.";
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }

                /* Modify finish time of that pipeline execution */
                //Not yet

                /* Remove entry for this pipeline in map */
                //Not yet

                /* Return ACK */
                t.sendResponseHeaders(200, 0);
                t.getResponseBody().close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private EsSlave es_slave_logs, es_slave_pipelines;
    private HashMap<String, PipelineRecord> pipelines;
    private Gson gson;
    private PrintWriter log_file;

    public PipelineMonitor() throws IOException
    {
        es_slave_logs = new EsSlave(Config.ESS_INDEX, Config.ESS_LOG_TYPE, Config.ESS_LOG_MAPPING_FILE);
        es_slave_pipelines = new EsSlave(Config.ESS_INDEX, Config.ESS_PIPELINE_TYPE, Config.ESS_PIPELINE_MAPPING_FILE);
        pipelines = new HashMap<String, PipelineRecord>();
        gson = new Gson();
        log_file = new PrintWriter(Config.PM_LOG_FILE);
    }

    public void initiate() throws IOException
    {
        System.out.println("Pipeline Server Initiating...");
        HttpServer server = HttpServer.create(new InetSocketAddress(Integer.valueOf(Config.PM_PORT)), 0);
        HttpContext cont_start = server.createContext(Config.CONT_START_PIPELINE, new Start());
        HttpContext cont_reqds = server.createContext(Config.CONT_REQUIRE_DATA_STORAGE, new ReqDS());
        //HttpContext cont_eval = server.createContext(Config.CONT_EVALUATION, new Eval(this));
        HttpContext cont_terminate = server.createContext(Config.CONT_TERMINATE_PIPELINE, new Terminate());

        /* Rebuild ESS Index and Type */
        try
        {
            es_slave_pipelines.deleteIndex();
            es_slave_pipelines.addIndex();
            System.out.println("ESS index: " + Config.ESS_INDEX + " rebuilt");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.err.println("ESS index rebuild failed, create new index: " + Config.ESS_INDEX);
            es_slave_pipelines.addIndex();
        }
        es_slave_logs.addType();
        es_slave_pipelines.addType();

        server.setExecutor(null);
        server.start();
        System.out.printf("Pipeline Server Initiated. Listening on port %s\n", Config.PM_PORT);
        System.out.println("\tInteracting with ES server at: " + es_slave_pipelines.getESSLocation());
        System.out.printf("\tLogging In: '%s'\n", Config.PM_LOG_FILE);
    }
}
