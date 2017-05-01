package com.logprov.pipeline;

import com.google.gson.Gson;
import com.logprov.Config;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author  Ruoyu Wang
 * @version 3.5
 * Date:    2017.04.29
 *
 *   Main server of LogProv system. It monitors executions of all pig pipelines, storing intermediate data and
 * performing operation level well-functioning confidence check. It responds requests lodged from users to query for
 * pipeline parameters.
 *
 * File Hierarchy & Format:
 *   [HDFS path]
 *    ┣━[PID[1]]
 *    ┃  ┣━[idx[1]_var[1]]
 *    ┃  ┃  ┣━part_0      => part 0 of intermediate data, 'csv' format
 *    ┃  ┃  ┣━...                                                       ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
 *    ┃  ┃  ┣━part_[n]    => part n of intermediate data, 'csv' format  ┃[bucket#[1]:[probability[1]]┃
 *    ┃  ┃  ┣━_DISTRIBUTION_COL_0      => distribution of column 0      ┃...                         ┃
 *    ┃  ┃  ┣━...                                                    => ┃[bucket#[b]:[probability[b]]┃
 *    ┃  ┃  ┣━_DISTRIBUTION_COL_[k]    => distribution of column k      ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
 *    ┃  ┃  ┗━(*)_CORRUPTED            => empty indicator file, exists only when confidence is lower than
 *    ┃  ┣━...                            threshold
 *    ┃  ┗━[idx[v]_var[v]]
 *    ┃     ┗━...
 *    ┣━...
 *    ┣━[PID[p]]
 *    ┃  ┗━..                                                      ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
 *    ┗━_CONFIDENCE_BUCKETS                                        ┃[column index[0]]:[base[0]],[setp[0]]┃
 *       ┣━[var[1]]    => var[1] distribution histogram setting    ┃...                                  ┃
 *       ┣━...                                                  => ┃[column index[i]]:[base[i]],[step[i]]┃
 *       ┗━[var[v]]    => var[v] distribution histogram setting    ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
 */
public class PipelineMonitor {

    /*
     *   Auxiliary structure used for recording allocation information of pipeline intermediate data generated from Pig
     * Latin variables.
     */
    private class VarAllocInfo{
        public String index;    // Variable index
        public int alloc_num;   // Number of assigned output files recording data held by the
                                // variable

        private VarAllocInfo(){}

        public VarAllocInfo(String idx)
        {
            index = idx;
            alloc_num = 0;
        }
    }

    /*
     *   Auxiliary structure used for recording pipeline execution information, including pipeline storage path,
     * variables.
     */
    private class PipelineRecord{
        String hdfs_path;                   // HDFS path to directories and files recording
                                            // pipeline intermediate data
        HashMap<String, VarAllocInfo> vars; // Recorded variables in the execution

        private PipelineRecord(){}

        public PipelineRecord(String hdfs_path)
        {
            this.hdfs_path = hdfs_path;
            vars = new HashMap<String, VarAllocInfo>();
        }
    }

    /*
     *   Http handler for requests to start a new pipeline execution. It records pipeline execution details in memory
     * for later requests, assign an unique ID for this pipeline and return this PID to user.
     *
     * Context URL:
     *   See "com.logprov.Config.CONT_START_PIPELINE"
     *
     * Input(in lines):
     *   1. HDFS Path
     *   2. Remarks(Extra info) of the execution
     *
     * Ouput(in lines):
     *   [Respond Code 200]
     *   1. Pipeline ID(PID)
     */
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
                //es_slave_pipelines.send(gson.toJson(pinfo));
                Response response = es_client.performRequest("POST", String.format("/%s/%s", Config.ESS_INDEX, Config.ESS_PIPELINE_TYPE),
                        Collections.<String, String>emptyMap(),
                        new NStringEntity(gson.toJson(pinfo)));

                System.out.println(String.format("%s: %s", EntityUtils.toString(response.getEntity()), response.toString()));
                System.out.println(String.format("New PID: %s\n", pinfo.pid));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    /*
     *   Http handler for requests to allocate a new HDFS path correspond to certain variable to write down intermediate
     * data generated during pipeline execution. It looks up for variable allocation information in certain pipeline
     * execution, assign a new path and return it to user.
     *
     * Context URL:
     *   See "com.logprov.Config.CONT_REQUIRE_DATA_STORAGE"
     *
     * Input(in lines):
     *   1. LogLine JSON string('pid', 'srcvar', 'operation', 'dstvar', 'start', 'column_type' and 'inspect_columns'
     * should already be assigned; so should 'srcidx' and 'dstidx' if this is from a loader.)
     *
     * Output(in liens):
     *   [Respond Code 200]
     *   1. A Specific HDFS File Path
     */
    private class ReqDS implements HttpHandler {

        public void handle(HttpExchange t)
        {
            try
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
                LogLine log = gson.fromJson(in.readLine(), LogLine.class);
                in.close();
                boolean from_loader = (null != log.dstidx);

                /* Check validity */
                PipelineRecord precord = pipelines.get(log.pid);
                if (null == precord)
                {
                    in.close();
                    String msg = String.format("Invalid PID: '%s'; No such pipeline running currently.", log.pid);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    System.err.println("REQDS: " + msg);
                    return;
                }

                /* Check allocate new entry or use old one */
                VarAllocInfo vinfo = precord.vars.get(log.dstvar);
                String storage_path = precord.hdfs_path;
                if (null == vinfo)
                {
                    /* Generate index */
                    if (from_loader)
                    {
                        log.srcidx = "-";
                        log.srcvar = "-";
                    }
                    else
                        log.dstidx = UUID.randomUUID().toString();

                    /* Record allocation mapping */
                    vinfo = new VarAllocInfo(log.dstidx);
                    precord.vars.put(log.dstvar, vinfo);

                    /* Return storage path */
                    if (from_loader)
                        storage_path = log.dstidx;
                    else
                        storage_path += String.format("/%s/%s_%s/%s", log.pid, log.dstidx, log.dstvar,
                                "part_" + Integer.toString(vinfo.alloc_num));
                    vinfo.alloc_num++;
                    t.sendResponseHeaders(200, storage_path.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(storage_path.getBytes());
                    out.close();

                    /* Complete srcidx if variable is not generated from loader and write to ESS */
                    if (!from_loader)
                    {
                        String srcvars[] = log.srcvar.split(",");
                        VarAllocInfo tmpvinfo = precord.vars.get(srcvars[0]);
                        if (null == tmpvinfo)
                            throw new IOException("ReqDS:: Variable processing order violated.");
                        log.srcidx = tmpvinfo.index;
                        for (int i = 1; i < srcvars.length; i++)
                        {
                            tmpvinfo = precord.vars.get(srcvars[i]);
                            if (null == tmpvinfo)
                                throw new IOException("ReqDS:: Variable processing order violated.");
                            log.srcidx += ',' + tmpvinfo.index;
                        }
                    }

                    es_client.performRequest("POST", String.format("/%s/%s", Config.ESS_INDEX, Config.ESS_LOG_TYPE),
                            Collections.<String, String>emptyMap(),
                            new NStringEntity(gson.toJson(log), ContentType.APPLICATION_JSON));
                }
                else
                {
                    /* Return storage path */
                    if (from_loader)
                        storage_path = log.dstidx;
                    else
                        storage_path += String.format("/%s/%s_%s/%s", log.pid, vinfo.index, log.dstvar,
                            "part_" + Integer.toString(vinfo.alloc_num));
                    vinfo.alloc_num++;
                    t.sendResponseHeaders(200, storage_path.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(storage_path.getBytes());
                    out.close();
                }
                System.out.println(String.format("ALLOC<%s>: %s, %s, %d\n", log.dstvar, log.pid, vinfo.index, vinfo.alloc_num));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    /*
     *   Http handler to terminate a pipeline execution after a pipeline finishes. It modifies pipeline information in
     * Elasticsearch and remove execution record from memory. At the end, it performs operation level confidence check
     * on every operation to make sure that every operation is well-functioning.
     *
     * Context URL:
     *   See "com.logprov.Config.CONT_TERMINATE_PIPELINE"
     *
     * Input(in lines):
     *   1. Pipeline ID(PID)
     *
     * Output(in lines):
     *   [Respond Code 200]
     *   None
     */
    private class Terminate implements HttpHandler{

        /*
         *   This class is used to accumulate elements and calculate their distribution
         */
        private class DistributionCalculator
        {
            private double start;
            private double step;
            private boolean is_num_type;
            private int cnt;
            private HashMap<Integer, Integer> frequency;
            private HashMap<Integer, Double> distribution;

            private DistributionCalculator(){}

            public DistributionCalculator(double start, double step, String type)
            {
                this.start = start;
                this.step = step;
                this.is_num_type = ("n".equals(type));
                this.cnt = 0;
                frequency = new HashMap<Integer, Integer>();
                distribution = null;
            }

            public void add(String element)
            {
                distribution = null;
                cnt++;
                double val;

                if (is_num_type)
                    val = Double.valueOf(element);
                else
                    val = element.hashCode();

                double diff = val - start;
                int bucket = (int)Math.floor(diff / step);
                Integer f = frequency.get(bucket);
                if (null == f)
                    frequency.put(bucket, 1);
                else
                    frequency.put(bucket, f + 1);
            }

            public void fromFile(BufferedReader reader) throws IOException
            {
                distribution = new HashMap<Integer, Double>();
                String line;
                while (null != (line = reader.readLine()))
                {
                    String[] paras = line.split(":");
                    distribution.put(Integer.valueOf(paras[0]), Double.valueOf(paras[1]));
                }
            }

            public HashMap<Integer, Double> calculate()
            {
                if (null != distribution)
                    return distribution;

                distribution = new HashMap<Integer, Double>();
                for (Map.Entry<Integer, Integer> entry : frequency.entrySet())
                    distribution.put(entry.getKey(), (double)(entry.getValue()) / (double)cnt);
                return distribution;
            }
        }

        public void handle(HttpExchange t)
        {
            try
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
                String pid = in.readLine();
                in.close();

                /* Check validity */
                PipelineRecord precord = pipelines.get(pid);
                if (null == precord)
                {
                    String msg = String.format("Invalid PID '%s': No such pipeline is running currently.\n", pid);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    System.err.println("TERMINATE:: " + msg);
                    return;
                }

                /* Modify finish time of that pipeline execution */
                SearchResponse response = es_transport_client.prepareSearch(Config.ESS_INDEX)
                        .setTypes(Config.ESS_PIPELINE_TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.termQuery("pid", pid))
                        .get();
                SearchHit hits[] = response.getHits().getHits();
                if (0 < hits.length)
                {
                    System.out.println("HIT: " + hits[0].sourceAsString());
                    UpdateRequest request = new UpdateRequest();
                    request.index(Config.ESS_INDEX).type(Config.ESS_PIPELINE_TYPE).id(hits[0].getId())
                            .doc("finish", new Date().toString());
                    System.out.println("TERMINATE: " + es_transport_client.update(request).get().toString());
                }
                else
                {
                    String msg = String.format("Invalid PID '%s': No such pipeline recorded in Elasticsearch\n", pid);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    System.err.println("TERMINATE:: " + msg);
                    return;
                }

                /* Remove entry for this pipeline in map */
                pipelines.remove(pid);

                /* Return ACK */
                t.sendResponseHeaders(200, 0);
                t.getResponseBody().close();

                /* Perform divergence check */
                checkConfidence(pid, precord.hdfs_path);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        private void checkConfidence(String pid, String hdfs_path)
                throws IOException, InterruptedException, ExecutionException
        {
            /* Fetch log entries of all variables from ES */
            SearchResponse response = es_transport_client.prepareSearch(Config.ESS_INDEX)
                    .setTypes(Config.ESS_LOG_TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("pid", pid))
                    .get();
            SearchHit hits[] = response.getHits().getHits();

            /* Calculate confidence on each variable */
            for (SearchHit hit : hits)
            {
                /* Fetch distribution meta info */
                LogLine log = gson.fromJson(hit.getSourceAsString(), LogLine.class);

                if ("".equals(log.inspected_columns))
                {
                    System.out.printf("CONFIDENCE:: No column need inspecting in variable '%s'\n", log.dstvar);
                    continue;
                }
                String[] column_types = log.column_type.split(",");
                String[] columns_idx_str = log.inspected_columns.split(",");

                FileStatus[] history_dirs = getHistoryDirs(hdfs_path, log);
                if (2 > history_dirs.length)  //not enough history
                {
                    System.err.printf("CONFIDENCE:: Not enough history records(%d history data found) for variable " +
                            "'%s'\n", history_dirs.length, log.dstvar);
                    continue;
                }
                System.out.printf("CONFIDENCE::<%s> %d history data found.\n", log.dstvar, history_dirs.length);

                int[] column_idx = new int[columns_idx_str.length];
                HashMap<Integer, ArrayList<DistributionCalculator>> column_history =
                        new HashMap<Integer, ArrayList<DistributionCalculator>>();
                HashMap<Integer, DistributionCalculator> sample = new HashMap<Integer, DistributionCalculator>();
                for (int i = 0; i < column_idx.length; i++)
                {
                    column_idx[i] = Integer.valueOf(columns_idx_str[i]);
                    column_history.put(column_idx[i], new ArrayList<DistributionCalculator>());
                }
                HashMap<Integer, String> buckets = getColumnBuckets(hdfs_path, log, column_idx, column_types,
                        history_dirs[0]);

                /* Access every history dir to calculate(or read) history distributions */
                for (FileStatus dir : history_dirs)
                {
                    if (!dir.isDirectory())
                        continue;
                    if (hdfs.exists(new Path(String.format("%s/%s", dir.getPath().toString(),
                            Config.CFD_INDICATOR_FILE))))
                        continue;

                    DistributionCalculator[] calculators = new DistributionCalculator[column_idx.length];
                    for (int i = 0; i < column_idx.length; i++)
                    {
                        int idx = column_idx[i];
                        String[] paras = buckets.get(idx).split(",");
                        calculators[i] = new DistributionCalculator(Double.valueOf(paras[0]), Double.valueOf(paras[1]),
                                column_types[idx]);
                    }

                    /* Read existed files */
                    ArrayList<Integer> untracked_cols_list = new ArrayList<Integer>();
                    for (int i = 0; i < column_idx.length; i++)
                    {
                        int idx = column_idx[i];
                        Path dist_file_path = new Path(String.format("%s/%s%d", dir.getPath().toString(),
                                Config.CFD_DIST_FILE_PREFIX, idx));
                        if (hdfs.exists(dist_file_path))
                        {
                            BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(dist_file_path)));
                            calculators[i].fromFile(reader);
                            reader.close();
                        }
                        else
                        {
                            untracked_cols_list.add(i);
                        }
                    }

                    /* Calculate untracked distributions and write into files */
                    if (0 < untracked_cols_list.size())
                    {
                        Integer[] untracked_cols = untracked_cols_list.toArray(new Integer[0]);
                        FileStatus[] fragments = hdfs.globStatus(new Path(dir.getPath().toString() + "/part_*"));
                        for (FileStatus fs : fragments)
                        {
                            BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath())));
                            String line;
                            while (null != (line = reader.readLine()))
                            {
                                String[] cols = line.split(",");
                                for (int i = 0; i < untracked_cols.length; i++)
                                {
                                    int idx_idx = untracked_cols[i];
                                    calculators[idx_idx].add(cols[column_idx[idx_idx]]);
                                }
                            }
                            reader.close();
                        }

                        for (int i = 0; i < untracked_cols.length; i++)
                        {
                            int idx_idx = untracked_cols[i];
                            HashMap<Integer, Double> dist = calculators[idx_idx].calculate();

                            Path dist_file_path = new Path(String.format("%s/%s%d", dir.getPath().toString(),
                                    Config.CFD_DIST_FILE_PREFIX, column_idx[idx_idx]));
                            PrintWriter writer = new PrintWriter(hdfs.create(dist_file_path));
                            for (Map.Entry<Integer, Double> entry : dist.entrySet())
                                writer.println(String.format("%d:%g", entry.getKey(), entry.getValue()));
                            writer.close();
                        }
                    }

                    for (int i = 0; i < column_idx.length; i++)
                        column_history.get(column_idx[i]).add(calculators[i]);
                }

                /* Access sample dir to calculate sample distributions */
                String sample_path = String.format("%s/%s/%s_%s", hdfs_path, log.pid, log.dstidx, log.dstvar);
                for (int i = 0; i < column_idx.length; i++)
                {
                    int idx = column_idx[i];
                    String[] paras = buckets.get(idx).split(",");
                    sample.put(idx, new DistributionCalculator(Double.valueOf(paras[0]), Double.valueOf(paras[1]),
                            column_types[idx]));
                }
                FileStatus[] fragments = hdfs.globStatus(new Path(sample_path + "/part_*"));
                System.out.printf("CONFIDENCE: %d parts found.\n", fragments.length);
                for (FileStatus fs : fragments)
                {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath())));
                    String line;
                    while (null != (line = reader.readLine()))
                    {
                        String[] cols = line.split(",");
                        for (int i = 0; i < column_idx.length; i++)
                        {
                            int idx = column_idx[i];
                            sample.get(idx).add(cols[idx]);
                        }
                    }
                    reader.close();
                }

                /* Calculate confidence and write distribution into files, write indicator file if applicable */
                log.confidence = "";
                for (int i = 0; i < column_idx.length; i++)
                {
                    int idx = column_idx[i];
                    ArrayList<DistributionCalculator> calculators = column_history.get(idx);
                    HashMap<Integer, Double>[] history_dists = new HashMap[calculators.size()];
                    for (int j = 0; j < history_dists.length; j++)
                        history_dists[j] = calculators.get(j).calculate();

                    HashMap<Integer, Double> sample_dist = sample.get(idx).calculate();
                    Path dist_file_path = new Path(String.format("%s/%s%d", sample_path, Config.CFD_DIST_FILE_PREFIX,
                            idx));
                    PrintWriter writer = new PrintWriter(hdfs.create(dist_file_path));
                    for (Map.Entry<Integer, Double> entry : sample_dist.entrySet())
                        writer.println(String.format("%d:%g", entry.getKey(), entry.getValue()));
                    writer.close();

                    double cfd = confidence(history_dists, sample_dist);
                    if (0 == i)
                        log.confidence += String.format("%d:%g", idx, cfd);
                    else
                        log.confidence += String.format(",%d:%g", idx, cfd);
                    if (cfd < Config.CFD_THRESHOLD)
                        hdfs.create(new Path(String.format("%s/%s", sample_path, Config.CFD_INDICATOR_FILE)));
                }

                System.out.printf("CONFIDENCE::<%s> %s\n", log.dstvar, log.confidence);

                /* Modify log in ESS */
                UpdateRequest request = new UpdateRequest();
                request.index(Config.ESS_INDEX).type(Config.ESS_LOG_TYPE).id(hit.getId())
                        .doc("confidence", log.confidence);
                System.out.println("CONFIDENCE: " + es_transport_client.update(request).get().toString());
            }
        }

        private FileStatus[] getHistoryDirs(String hdfs_path, LogLine log) throws IOException
        {
            String dir_path = String.format("%s/*/*_%s", hdfs_path, log.dstvar);
            String sample_path = String.format("%s/%s/%s_%s", hdfs_path, log.pid, log.dstidx, log.dstvar);
            FileStatus[] dirs = hdfs.globStatus(new Path(dir_path));
            ArrayList<FileStatus> list = new ArrayList<FileStatus>();
            for (FileStatus fs : dirs)
            {
                if (!fs.getPath().toString().contains(sample_path))
                    list.add(fs);
            }
            return list.toArray(new FileStatus[0]);
        }

        private HashMap<Integer, String> getColumnBuckets(String hdfs_path, LogLine log, int[] col_idx,
                                                          String[] col_type, FileStatus dir)
                throws IOException
        {
            HashMap<Integer, String> buckets = new HashMap<Integer, String>();
            Path bkt_file_path = new Path(String.format("%s/%s/%s", hdfs_path, Config.CFD_BKT_DIR, log.dstvar));
            if (hdfs.exists(bkt_file_path))
            {
                BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(bkt_file_path)));
                String line;
                while (null != (line = reader.readLine()))
                {
                    String[] tmp = line.split(":");
                    buckets.put(Integer.valueOf(tmp[0]), tmp[1]);
                }
                reader.close();
            }

            /* Check whether there is any untracked column */
            ArrayList<Integer> tmp_list = new ArrayList<Integer>();
            for (int i : col_idx)
            {
                if (!buckets.containsKey(i))
                    tmp_list.add(i);
            }

            /* Complete the map if necessary */
            if (!tmp_list.isEmpty())
            {
                Integer[] unrecorded_idx = tmp_list.toArray(new Integer[0]);
                double[] max = new double[unrecorded_idx.length];
                double[] min = new double[unrecorded_idx.length];
                for (int i = 0; i < unrecorded_idx.length; i++)
                {
                    max[i] = Double.MIN_VALUE;
                    min[i] = Double.MAX_VALUE;
                }

                FileStatus[] fragments = hdfs.globStatus(new Path(dir.getPath().toString() + "/part_*"));
                for (FileStatus fs : fragments)
                {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath())));
                    String line;
                    while (null != (line = reader.readLine()))
                    {
                        String[] cols = line.split(",");
                        for (int i = 0; i < unrecorded_idx.length; i++)
                        {
                            int idx = unrecorded_idx[i];
                            double val = ("n".equals(col_type[idx]))? Double.valueOf(cols[idx]):cols[idx].hashCode();
                            max[i] = (val > max[i])?val:max[i];
                            min[i] = (val < min[i])?val:min[i];
                        }
                    }
                    reader.close();
                }
                for (int i = 0; i < unrecorded_idx.length; i++)
                {
                    int idx = unrecorded_idx[i];
                    double start = min[i];
                    double step = calStep(max[i], min[i]);
                    buckets.put(idx, String.format("%g,%g", start, step));
                }

                /* Modify bucket file */
                PrintWriter writer = new PrintWriter(hdfs.create(bkt_file_path));
                for (Map.Entry<Integer, String> entry : buckets.entrySet())
                    writer.println(String.format("%d:%s", entry.getKey(), entry.getValue()));
                writer.close();
            }
            return buckets;
        }

        private double calStep(double max, double min)
        {
            return (max - min) / 1000;
        }

        private double confidence(HashMap<Integer, Double>[] populations, HashMap<Integer, Double> sample)
        {
            HashMap<Integer, Double> M = new HashMap<Integer, Double>();
            HashMap<Integer, Double>[] P = populations;
            HashMap<Integer, Double> S = sample;

            /* Calculate distribution for population and sample */
            for (int i = 0; i < populations.length; i++)
            {
                for (Map.Entry<Integer, Double> entry : P[i].entrySet())
                {
                    Double p = M.get(entry.getKey());
                    if (null == p)
                        M.put(entry.getKey(), entry.getValue());
                    else
                        M.put(entry.getKey(), p + entry.getValue());
                }
            }

            for (Map.Entry<Integer, Double> entry : M.entrySet())
            {
                entry.setValue(entry.getValue() / populations.length);
            }

            /* Calculate distribution for JSDs of normal populations */
            SummaryStatistics statistics = new SummaryStatistics();
            for (int i = 0; i < populations.length; i++)
                statistics.addValue(JSD(P[i], M));
            NormalDistribution JSD_distribution = new NormalDistribution(statistics.getMean(),
                    statistics.getStandardDeviation());

            /* Calculate JSD for sample distribution and population distribution */
            /*for (int i = 0; i < populations.length; i++)
            {
                System.out.printf("<JSD[%d]:>%g\n", i, JSD(P[i], M));
            }
            System.out.printf("<JSD[]:>%g\n", JSD(S, M));*/
            return JSD_distribution.density(JSD(S, M));
        }

        private double JSD(HashMap<Integer, Double> P, HashMap<Integer, Double> Q)
        {
            HashMap<Integer, Double> M = new HashMap<Integer, Double>();
            for (Map.Entry<Integer, Double> entry : P.entrySet())
            {
                Integer i = entry.getKey();
                Double prob = M.get(i);
                if (null == prob)
                    M.put(i, entry.getValue());
                else
                    M.put(i, prob + entry.getValue());
            }

            for (Map.Entry<Integer, Double> entry : Q.entrySet())
            {
                Integer i = entry.getKey();
                Double prob = M.get(i);
                if (null == prob)
                    M.put(i, entry.getValue());
                else
                    M.put(i, prob + entry.getValue());
            }

            for (Map.Entry<Integer, Double> entry : M.entrySet())
            {
                entry.setValue(entry.getValue() / 2);
            }

            double d = 0;
            for (Map.Entry<Integer, Double> entry : P.entrySet())
            {
                d += entry.getValue() * Math.log(entry.getValue() / M.get(entry.getKey()));
            }
            for (Map.Entry<Integer, Double> entry : Q.entrySet())
            {
                d += entry.getValue() * Math.log(entry.getValue() / M.get(entry.getKey()));
            }
            d /= 2;
            return d;
        }
    }

    /*
     *   Http handler to evaluate certain pipeline paths ended by one particular variable. It looks up the paths from
     * ES and memory, then sends HDFS file path to the oracle and get an result score, then add the score to all
     * involved components.
     *
     * Context URL:
     *   See "com.logprov.Config.CONT_EVALUATION"
     *
     * Input(in lines):
     *   1. Pipeline ID(PID)
     *   2. Variable Name
     *   3. Oracle URL
     *
     * Output(in lines):
     *   [Respond Code 200]
     *   1. Acknowledge Message String
     */
    private class Evaluate implements HttpHandler {

        private class LogInfo{
            String eid = null;
            LogLine log = null;
        }

        public void handle(HttpExchange t)
        {
            try
            {
                /* Read parameters */
                BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
                String pid = in.readLine();
                String varname = in.readLine();
                String oracle_address = in.readLine();
                in.close();

                /* Get the file path */
                String path;
                PipelineRecord precord = pipelines.get(pid);
                if (null == precord)
                {
                    /* Pipeline terminated or doesn't exist */
                    String hdfspath;
                    SearchResponse response = es_transport_client.prepareSearch(Config.ESS_INDEX)
                            .setTypes(Config.ESS_PIPELINE_TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setQuery(QueryBuilders.termQuery("pid", pid))
                            .get();
                    SearchHit hits[] = response.getHits().getHits();
                    if (0 < hits.length)
                    {
                        hdfspath = gson.fromJson(hits[0].getSourceAsString(), PipelineRecord.class).hdfs_path;
                    }
                    else
                    {
                        /* Pipeline doesn't exist */
                        String msg = String.format("Bad PID: Pipeline '%s' does not exist.\n", pid);
                        t.sendResponseHeaders(400, msg.getBytes().length);
                        OutputStream out = t.getResponseBody();
                        out.write(msg.getBytes());
                        out.close();
                        return;
                    }

                    response = es_transport_client.prepareSearch(Config.ESS_INDEX)
                            .setTypes(Config.ESS_LOG_TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("pid", pid))
                                    .must(QueryBuilders.termQuery("dstvar", varname)))
                            .get();
                    hits = response.getHits().getHits();
                    if (0 < hits.length)
                    {
                        path = String.format("%s/%s/%s/", hdfspath, pid,
                                gson.fromJson(hits[0].getSourceAsString(), LogLine.class).dstidx);
                    }
                    else
                    {
                        /* Variable doesn't exist */
                        String msg = String.format("Bad Variable Name: '%s' hasn't been inspected.\n", varname);
                        t.sendResponseHeaders(400, msg.getBytes().length);
                        OutputStream out = t.getResponseBody();
                        out.write(msg.getBytes());
                        out.close();
                        return;
                    }
                }
                else
                {
                    VarAllocInfo vinfo = precord.vars.get(varname);
                    if (null == vinfo)
                    {
                        /* No such variable */
                        String msg = String.format("Bad Variable Name: '%s' hasn't been inspected.\n", varname);
                        t.sendResponseHeaders(400, msg.getBytes().length);
                        OutputStream out = t.getResponseBody();
                        out.write(msg.getBytes());
                        out.close();
                        return;
                    }
                    path = String.format("%s/%s/%s/", precord.hdfs_path, pid, vinfo.index);
                }

                /* Send the file path to the Oracle and wait for the feed back */
                URL url = new URL(oracle_address);
                HttpURLConnection con = (HttpURLConnection)url.openConnection();
                con.setRequestMethod("GET");
                con.setDoInput(true);
                con.setDoOutput(true);
                OutputStream out = con.getOutputStream();
                out.write(path.getBytes());
                out.close();

                /* Oracle reads data and return a judgement */
                int resp_code = con.getResponseCode();
                double feed_back;
                if (200 == resp_code)
                {
                    in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                    feed_back = Double.valueOf(in.readLine());
                }
                else
                {
                    /* Feed back error */
                    String msg = String.format("Oracle Error: Response Code: %d\n", resp_code);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }

                /* Apply Elo-like scoring mechanism onto involved components */
                Scoring(pid, varname, feed_back);

                /* Return acknowledgement */
                String msg = String.format("Evaluation for variable '%s' in pipeline '%s' finished. Feedback: %f",
                        varname, pid, feed_back);
                t.sendResponseHeaders(200, msg.getBytes().length);
                out = t.getResponseBody();
                out.write(msg.getBytes());
                out.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        /*
         * Simple Elo-like method.
         */
        private void Scoring(String pid, String varname, double score) throws IOException
        {
            /* Get all component logs */
            HashMap<String, LogInfo> map = new HashMap<String, LogInfo>();
            LogInfo info = null;
            Queue<LogInfo> queue = new LinkedList<LogInfo>();
            Set<LogInfo> set = new HashSet<LogInfo>();
            SearchResponse response = es_transport_client.prepareSearch(Config.ESS_INDEX)
                    .setTypes(Config.ESS_LOG_TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("pid", pid))
                    .get();
            SearchHit hits[] = response.getHits().getHits();
            for (SearchHit hit : hits)
            {
                LogInfo tmpinfo = new LogInfo();
                tmpinfo.log = gson.fromJson(hit.getSourceAsString(), LogLine.class);
                tmpinfo.eid = hit.getId();
                map.put(tmpinfo.log.dstidx, tmpinfo);
                if (varname.equals(tmpinfo.log.dstvar))
                    info = tmpinfo;
            }

            if (null == info)
                throw new IOException(String.format("No variable named '%s' found in PID<%s>", varname, pid));

            /* Start backward propagation and add all modified components into a set */
            queue.add(info);
            while (!queue.isEmpty())
            {
                LogInfo tmpinfo = queue.remove();
                tmpinfo.log.score += score;
                set.add(tmpinfo);
                String src_indices[] = tmpinfo.log.srcidx.split(",");
                for (String srcidx : src_indices)
                {
                    tmpinfo = map.get(srcidx);
                    if (null != tmpinfo)
                        queue.add(tmpinfo);
                }
            }

            /* Update those modified logs */
            for (LogInfo tmpinfo : set)
            {
                String json = String.format("{\"doc\":{\"score\":%f}}", tmpinfo.log.score);
                es_client.performRequest("POST", String.format("/%s/%s/%s/_update", Config.ESS_INDEX,
                        Config.ESS_LOG_TYPE, tmpinfo.eid), Collections.<String, String>emptyMap(),
                        new NStringEntity(json, ContentType.APPLICATION_JSON));
            }
        }
    }

    /*
     *   Http handler to answer search request. It searches from ES and integrate the results and send back to user.
     *
     * Context URL:
     *   See "com.logprov.Config.CONT_SEARCH"
     *
     * Input(in lines):
     *   1. Search Type: See "this.FOR_DATA", "this.FOR_META", "this.FOR_SEMANTICS"
     *   2. [Multi-line]SQL Query Script
     *
     * Output(in lines):
     *   1. (1)[DATA][Multi-line]Data Content in CSV format
     *      (2)[META][Multi-line]Result CSV File
     *      (3)[SEMANTICS][Multi-line]Result CSV file
     */
    private class Search implements HttpHandler{

        static final String FOR_DATA = "data";
        static final String FOR_META = "meta";
        static final String FOR_SEMANTICS = "semantics";

        public void handle(HttpExchange t)
        {
            try
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
                String command = in.readLine();
                if (FOR_DATA.equals(command))
                {
                    handleData(t, in);
                }
                else if (FOR_META.equals(command))
                {
                    handleMeta(t, in);
                }
                else if (FOR_SEMANTICS.equals(command))
                {
                    handleSemantics(t, in);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        private void handleData(HttpExchange t, BufferedReader in) throws IOException
        {
            String title = "PID,Index,Dstvar,Data\n";
            HashMap<String, String> pid2hdfspath = new HashMap<String, String>();
            HashSet<String> exclude = new HashSet<String>();
            ArrayList<String> paths = new ArrayList<String>();
            ArrayList<String> infos = new ArrayList<String>();

            /* Get result info */
            String json = queryThroughSQLPlugin(in);

            /* Classify logs and pipelines */
            ESResponse es_response = gson.fromJson(json, ESResponse.class);
            for (ESResponse.Hit hit : es_response.hits.hits)
            {
                if (!Config.ESS_INDEX.equals(hit._index))
                {
                    String msg = String.format("Invalid ES index: '%s'. Should be '%s' in current LogProv configuration."
                            , hit._index, Config.ESS_INDEX);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }
                if (Config.ESS_LOG_TYPE.equals(hit._type))
                {
                    continue;
                }
                else if (Config.ESS_PIPELINE_TYPE.equals(hit._type))
                {
                    exclude.add(hit._source.pid);

                    SearchResponse response = es_transport_client.prepareSearch(Config.ESS_INDEX)
                            .setTypes(Config.ESS_LOG_TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setQuery(QueryBuilders.termQuery("pid", hit._source.pid))
                            .get();
                    for (SearchHit hit_tmp : response.getHits().getHits())
                    {
                        LogLine log = gson.fromJson(hit_tmp.getSourceAsString(), LogLine.class);
                        if ("-".equals(log.srcvar))  // Log for loader
                            paths.add(log.dstidx);
                        else
                            paths.add(String.format("%s/%s/%s_%s/", hit._source.hdfs_path, log.pid, log.dstidx, log.dstvar));
                        infos.add(String.format("%s,\"%s\",\"%s\",", log.pid, log.dstidx, log.dstvar));
                    }
                }
                else
                {
                    String msg = String.format("Invalid ES type: '%s'. Should be '%s' or '%s' in current LogProv configuration."
                            , hit._type, Config.ESS_LOG_TYPE, Config.ESS_PIPELINE_TYPE);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }
            }
            for (ESResponse.Hit hit : es_response.hits.hits)
            {
                if (Config.ESS_LOG_TYPE.equals(hit._type))
                {
                    if (!exclude.contains(hit._source.pid))
                    {
                        String hdfspath = pid2hdfspath.get(hit._source.pid);
                        if (null == hdfspath)
                        {
                            SearchResponse response = es_transport_client.prepareSearch(Config.ESS_INDEX)
                                    .setTypes(Config.ESS_PIPELINE_TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                                    .setQuery(QueryBuilders.termQuery("pid", hit._source.pid))
                                    .get();
                            if (0 == response.getHits().getHits().length)
                            {
                                String msg = String.format("Invalid PID: '%s'. This PID does not exist.", hit._source.pid);
                                t.sendResponseHeaders(400, msg.getBytes().length);
                                OutputStream out = t.getResponseBody();
                                out.write(msg.getBytes());
                                out.close();
                                return;
                            }
                            hdfspath = gson.fromJson(response.getHits().hits()[0].getSourceAsString(), PipelineRecord.class).hdfs_path;
                            pid2hdfspath.put(hit._source.pid, hdfspath);
                        }

                        paths.add(String.format("%s/%s/%s_%s/", hdfspath, hit._source.pid, hit._source.dstidx, hit._source.dstvar));
                        infos.add(String.format("%s,\"%s\",\"%s\",", hit._source.pid, hit._source.dstidx, hit._source.dstvar));
                    }
                }
            }

            /* Construct paths and read files */
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();
            out.write(title.getBytes());

            int len = paths.size();
            for (int i = 0; i < len; i++)
            {
                String data = "";

                FileStatus[] file_status = hdfs.listStatus(new Path(paths.get(i)));
                for (int j = 0; j < file_status.length; j++)
                {
                    if (!file_status[j].isDirectory())
                    {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(file_status[j].getPath())));
                        String line;
                        while (null != (line = reader.readLine()))
                            data += line + "\\\\";
                    }
                    out.write((infos.get(i) + "\"" + data +"\"\n").getBytes());
                }
            }
            out.close();
        }

        private void handleMeta(HttpExchange t, BufferedReader in)
                throws IOException, NoSuchFieldException, IllegalAccessException
        {
            /* Construct pipeline table title */
            String pipeline_title = "";
            Field[] fields = PipelineInfo.class.getFields();
            boolean first = true;
            for (int i = 0; i < fields.length; i++)
            {
                if (Modifier.isPublic(fields[i].getModifiers()))
                {
                    if (first)
                    {
                        first = false;
                        pipeline_title += fields[i].getName();
                    }
                    else
                    {
                        pipeline_title += ',' + fields[i].getName();
                    }
                }
            }

            /* Construct log line table title */
            String log_title = "";
            fields = LogLine.class.getFields();
            first = true;
            for (int i = 0; i < fields.length; i++)
            {
                if (Modifier.isPublic(fields[i].getModifiers()))
                {
                    if (first)
                    {
                        first = false;
                        log_title += fields[i].getName();
                    }
                    else
                    {
                        log_title += ',' + fields[i].getName();
                    }
                }
            }

            /* Integrate returned results */
            ArrayList<ESResponse.Source> logs_meta = new ArrayList<ESResponse.Source>();
            ArrayList<ESResponse.Source> pipelines_meta = new ArrayList<ESResponse.Source>();

            String json = queryThroughSQLPlugin(in);
            ESResponse es_response = gson.fromJson(json, ESResponse.class);
            for (ESResponse.Hit hit : es_response.hits.hits)
            {
                if (!Config.ESS_INDEX.equals(hit._index))
                {
                    String msg = String.format("Invalid ES index: '%s'. Should be '%s' in current LogProv configuration."
                            , hit._index, Config.ESS_INDEX);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }
                if (Config.ESS_LOG_TYPE.equals(hit._type))
                {
                    logs_meta.add(hit._source);
                }
                else if (Config.ESS_PIPELINE_TYPE.equals(hit._type))
                {
                    pipelines_meta.add(hit._source);
                }
                else
                {
                    String msg = String.format("Invalid ES type: '%s'. Should be '%s' or '%s' in current LogProv configuration."
                            , hit._type, Config.ESS_LOG_TYPE, Config.ESS_PIPELINE_TYPE);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }
            }

            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();
            out.write(String.format("%s\n", pipeline_title).getBytes());
            for (ESResponse.Source source : pipelines_meta)
            {
                String line = "";
                fields = PipelineInfo.class.getFields();
                first = true;
                for (int i = 0; i < fields.length; i++)
                {
                    if (Modifier.isPublic(fields[i].getModifiers()))
                    {
                        if (first)
                        {
                            first = false;
                            line += "\"" +
                                    ESResponse.Source.class.getDeclaredField(fields[i].getName()).get(source)
                                    +"\"";
                        }
                        else
                        {
                            line += ',' + "\"" +
                                    ESResponse.Source.class.getDeclaredField(fields[i].getName()).get(source)
                                    +"\"";
                        }
                    }
                }
                out.write(String.format("%s\n", line).getBytes());
            }
            out.write('\n');
            out.write(String.format("%s\n", log_title).getBytes());
            for (ESResponse.Source source: logs_meta)
            {
                String line = "";
                fields = LogLine.class.getFields();
                first = true;
                for (int i = 0; i < fields.length; i++)
                {
                    if (Modifier.isPublic(fields[i].getModifiers()))
                    {
                        if (first)
                        {
                            first = false;
                            line += "\"" +
                                    ESResponse.Source.class.getDeclaredField(fields[i].getName()).get(source)
                                    +"\"";
                        }
                        else
                        {
                            line += ',' + "\"" +
                                    ESResponse.Source.class.getDeclaredField(fields[i].getName()).get(source)
                                    +"\"";
                        }
                    }
                }
                out.write(String.format("%s\n", line).getBytes());
            }
            out.close();
        }

        private void handleSemantics(HttpExchange t, BufferedReader in) throws IOException
        {
            /* List all PIDs */
            String json = queryThroughSQLPlugin(in);
            HashSet<String> pids = new HashSet<String>();

            ESResponse es_response = gson.fromJson(json, ESResponse.class);
            for (ESResponse.Hit hit : es_response.hits.hits)
            {
                if (!Config.ESS_INDEX.equals(hit._index))
                {
                    String msg = String.format("Invalid ES index: '%s'. Should be '%s' in current LogProv configuration."
                            , hit._index, Config.ESS_INDEX);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }
                if (Config.ESS_LOG_TYPE.equals(hit._type) || Config.ESS_PIPELINE_TYPE.equals(hit._type))
                {
                    pids.add(hit._source.pid);
                }
                else
                {
                    String msg = String.format("Invalid ES type: '%s'. Should be '%s' or '%s' in current LogProv configuration."
                            , hit._type, Config.ESS_LOG_TYPE, Config.ESS_PIPELINE_TYPE);
                    t.sendResponseHeaders(400, msg.getBytes().length);
                    OutputStream out = t.getResponseBody();
                    out.write(msg.getBytes());
                    out.close();
                    return;
                }
            }

            /* Integrate semantics for each PID */
            String title = "Srcvar,Operation,Dstvar\n";
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();

            for (String pid : pids)
            {
                SearchResponse response = es_transport_client.prepareSearch(Config.ESS_INDEX)
                        .setTypes(Config.ESS_LOG_TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.termQuery("pid", pid))
                        .get();

                out.write(String.format("PID,%s\n%s", pid,title).getBytes());
                for (SearchHit hit : response.getHits().getHits())
                {
                    LogLine log = gson.fromJson(hit.getSourceAsString(), LogLine.class);
                    out.write(String.format("\"%s\",\"%s\",\"%s\"\n", log.srcvar, log.operation, log.dstvar).getBytes());
                }
                out.write('\n');
            }
            out.close();
        }

        private String queryThroughSQLPlugin(BufferedReader in) throws IOException
        {
            URL url = new URL(String.format("%s://%s:%s/_sql", Config.ESS_PROTOCOL, Config.ESS_HOST, Config.ESS_PORT));
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("GET");
            con.setDoOutput(true);
            con.setDoInput(true);

            OutputStream out = con.getOutputStream();
            String line;
            while (null != (line = in.readLine()))
            {
                out.write(line.getBytes());
            }
            out.close();
            in.close();

            int resp_code = con.getResponseCode();
            if (400 == resp_code || 404 == resp_code) throw new IOException("Query via ES-SQL plugin failed!");
            String response = "";
            in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            while (null != (line = in.readLine()))
            {
                response += line;
            }
            in.close();
            return  response;
        }
    }

    /* Elasticsearch clients used for ES communication */
    private RestClient es_client;
    private TransportClient es_transport_client;
    /* Record for all started and running pipelines */
    private HashMap<String, PipelineRecord> pipelines;
    /* Gson tool to convert between JSON string and Java objects */
    private Gson gson;
    /* Log file */
    private PrintWriter log_file;
    /* HDFS connection */
    private FileSystem hdfs;

    public PipelineMonitor() throws IOException
    {
        es_client = RestClient.builder(new HttpHost(Config.ESS_HOST, Integer.valueOf(Config.ESS_PORT),
                Config.ESS_PROTOCOL)).build();
        es_transport_client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Config.ESS_HOST),
                        Integer.valueOf(Config.ESS_TRANSPORT_PORT)));
        pipelines = new HashMap<String, PipelineRecord>();
        gson = new Gson();
        log_file = new PrintWriter(Config.PM_LOG_FILE);

        /* Connect to HDFS */
        String hd_conf_dir = System.getenv("HADOOP_CONF_DIR");
        hd_conf_dir = "/home/trams/hadoop-2.7.2/etc/hadoop";  //for debugging only
        if (null == hd_conf_dir)
            throw new IOException("Environment variable 'HADOOP_CONF_DIR' not set!!");
        //hd_conf_dir = "/home/tramswang/hadoop-2.7.2/etc/hadoop";
        Configuration conf = new Configuration();
        conf.addResource(new Path(hd_conf_dir + "/core-site.xml"));
        conf.addResource(new Path(hd_conf_dir + "/hdfs-site.xml"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        //hdfs = FileSystem.get(URI.create("hdfs://master:8020/"), conf);
        hdfs = FileSystem.get(conf);
        System.out.println(String.format("Connecting to: %s<should be: %s>", hdfs.getName(), conf.get("fs.defaultFS")));
    }

    /* Initiate Server */
    public void initiate() throws IOException
    {
        System.out.println("Pipeline Server Initiating...");
        HttpServer server = HttpServer.create(new InetSocketAddress(Integer.valueOf(Config.PM_PORT)), 0);
        HttpContext cont_start = server.createContext(Config.CONT_START_PIPELINE, new Start());
        HttpContext cont_reqds = server.createContext(Config.CONT_REQUIRE_DATA_STORAGE, new ReqDS());
        HttpContext cont_eval = server.createContext(Config.CONT_EVALUATION, new Evaluate());
        HttpContext cont_terminate = server.createContext(Config.CONT_TERMINATE_PIPELINE, new Terminate());
        HttpContext cont_search = server.createContext(Config.CONT_SEARCH, new Search());

        /* Rebuild ESS Index and Type */
        Response response;
        try
        {
            response = es_client.performRequest("DELETE", '/' + Config.ESS_INDEX);
            System.out.println(String.format("%s: %s", EntityUtils.toString(response.getEntity()), response.toString()));
            response = es_client.performRequest("PUT", '/' + Config.ESS_INDEX);
            System.out.println(String.format("%s: %s", EntityUtils.toString(response.getEntity()), response.toString()));
            System.out.println("ESS index: " + Config.ESS_INDEX + " rebuilt");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.err.println("ESS index rebuild failed, create new index: " + Config.ESS_INDEX);
            response = es_client.performRequest("PUT", '/' + Config.ESS_INDEX);
            System.err.println(String.format("%s: %s", EntityUtils.toString(response.getEntity()), response.toString()));
        }

        BufferedReader in = new BufferedReader(new FileReader(Config.ESS_MAPPING_DIR + '/' + Config.ESS_LOG_MAPPING_FILE));
        String line;
        String mapping_json = "";
        while (null != (line = in.readLine()))
        {
            mapping_json += line;
        }
        in.close();
        mapping_json = mapping_json.replaceAll("\\s+", "");
        response = es_client.performRequest("PUT", String.format("/%s/_mapping/%s", Config.ESS_INDEX, Config.ESS_LOG_TYPE),
                Collections.<String, String>emptyMap(),
                new NStringEntity(mapping_json, ContentType.APPLICATION_JSON));
        System.out.println(String.format("%s: %s", EntityUtils.toString(response.getEntity()), response.toString()));

        in = new BufferedReader(new FileReader(Config.ESS_MAPPING_DIR + '/' + Config.ESS_PIPELINE_MAPPING_FILE));
        mapping_json = "";
        while (null != (line = in.readLine()))
        {
            mapping_json += line;
        }
        in.close();
        mapping_json = mapping_json.replaceAll("\\s+", "");
        response = es_client.performRequest("PUT", String.format("/%s/_mapping/%s", Config.ESS_INDEX, Config.ESS_PIPELINE_TYPE),
                Collections.<String, String>emptyMap(),
                new NStringEntity(mapping_json, ContentType.APPLICATION_JSON));
        System.out.println(String.format("%s: %s", EntityUtils.toString(response.getEntity()), response.toString()));
        response = es_client.performRequest("GET", "/_cat/indices?v");
        System.out.println(String.format("Cluster:\n%s: %s", EntityUtils.toString(response.getEntity()), response.toString()));

        /* Start server */
        server.setExecutor(null);
        server.start();
        System.out.printf("Pipeline Server Initiated. Listening on port %s\n", Config.PM_PORT);
        System.out.println("\tInteracting with ES server at: " + String.format("%s://%s:%s/%s", Config.ESS_PROTOCOL,
                Config.ESS_HOST, Config.ESS_PORT, Config.ESS_INDEX));
        System.out.printf("\tLogging In: '%s'\n", Config.PM_LOG_FILE);

    }

    @Override
    protected void finalize() throws Throwable {
        es_client.close();
        es_transport_client.close();
    }
}
