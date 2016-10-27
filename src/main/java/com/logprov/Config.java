package com.logprov;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;

/**
 * Created by babyfish on 16-10-26.
 */
public class Config {

    /**
     * Default Configurations
     */
    public static final String PM_PORT = "58888";
    public static final String PM_LOG_FILE = "PipelineMonitor.log";

    /* API Contexts */
    public static final String CONT_START_PIPELINE = "/_start";
    public static final String CONT_REQUIRE_DATA_STORAGE = "/_reqDS";
    public static final String CONT_EVALUATION = "/_eval";
    public static final String CONT_TERMINATE_PIPELINE = "/_terminate";

    /* Elasticsearch Connection Configurations */
    public static final String ESS_PROTOCOL = "http";
    public static final String ESS_HOST = "localhost";
    public static final String ESS_PORT = "9200";
    public static final String ESS_INDEX = "logprov";
    public static final String ESS_LOG_TYPE = "logs";
    public static final String ESS_PIPELINE_TYPE = "pipelines";
    public static final String ESS_MAPPING_DIR = "./Mapping";
    public static final String ESS_LOG_MAPPING_FILE = "logs.json";
    public static final String ESS_PIPELINE_MAPPING_FILE = "pipelines.json";

    private HashMap<String, String> pool;

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
}
