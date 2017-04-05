package com.logprov;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;

/**
 * Created by:  Ruoyu Wang
 * Date:        2017.04.05
 *
 *   System config parameters.
 */
public class Config {

    /* Default Configurations */
    public static final String PM_PORT = "58888";
    public static final String PM_LOG_FILE = "PipelineMonitor.log";
    public static final String PID_FILE = "pid.txt";

    /* Pipeline Status */
    public static final String PSTATUS_STARTED = "STARTED";
    public static final String PSTATUS_RUNNING = "RUNNING";
    public static final String PSTATUS_SUCCEEDED = "SUCCEEDED";
    public static final String PSTATUS_FAILED = "FAILED";

    /* API Contexts */
    public static final String CONT_START_PIPELINE = "/_start";
    public static final String CONT_REQUIRE_DATA_STORAGE = "/_reqDS";
    public static final String CONT_EVALUATION = "/_eval";
    public static final String CONT_TERMINATE_PIPELINE = "/_terminate";
    public static final String CONT_SEARCH = "/_search";

    /* Elasticsearch Connection Configurations */
    public static final String ESS_PROTOCOL = "http";
    public static final String ESS_HOST = "localhost";
    public static final String ESS_PORT = "9200";
    public static final String ESS_TRANSPORT_PORT = "9300";
    public static final String ESS_INDEX = "logprov";
    public static final String ESS_LOG_TYPE = "logs";
    public static final String ESS_PIPELINE_TYPE = "pipelines";
    public static final String ESS_MAPPING_DIR = "./Mapping";
    public static final String ESS_LOG_MAPPING_FILE = "logs.json";
    public static final String ESS_PIPELINE_MAPPING_FILE = "pipelines.json";

    private HashMap<String, String> pool;

    /* Construct a config object and load all constants into a map */
    public Config() throws IllegalAccessException
    {
        pool = new HashMap<String, String>();

        Field fields[] = Config.class.getFields();
        int mask = Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL;
        for (Field f : fields)
        {
            /* Check if the member is "public static final" then add to pool */
            int modifier = f.getModifiers();
            if (mask == (mask & modifier))
            {
                pool.put(f.getName(), (String)f.get(this));
            }
        }
    }

    /* TODO: Read a configuration file and modify default parameters */
    public void loadFile(String path)
    {
        ;
    }

    /* TODO: Change all public constant to private.*/
    /* TODO: And modify all codes elsewhere to use variables in the pool instead */
    public String get(String var)
    {
        return pool.get(var);
    }

    /* TODO: Show all paras */
    public void show()
    {
        ;
    }
}
