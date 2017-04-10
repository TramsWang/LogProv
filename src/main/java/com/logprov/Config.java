package com.logprov;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * @author  Ruoyu Wang
 * @version 1.1
 * Date:    2017.04.10
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

    //private HashMap<String, String> pool;
    //private String file_path = "./Config.conf";

    /* Construct a config object and load all constants into a map */
    public Config() throws IllegalAccessException
    {
        /*
        pool = new HashMap<String, String>();

        Field fields[] = Config.class.getFields();
        int mask = Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL;
        for (Field f : fields)
        {
            /* Check if the member is "public static final" then add to pool
            int modifier = f.getModifiers();
            if (mask == (mask & modifier))
            {
                pool.put(f.getName(), (String)f.get(this));
            }
        }

        loadFile(file_path);

        show();*/
    }

    /* Read a configuration file and modify default parameters */
    /*public void loadFile(String path)
    {
        String line;
        int cnt = 0;
        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            while (null != (line = reader.readLine()))
            {
                cnt++;
                line = line.replaceAll("\\s+", "");
                if ("".equals(line))
                    continue;

                String paras[] = line.split("=");
                if (pool.containsKey(paras[0]))
                {
                    pool.put(paras[0], paras[1]);
                }
                else
                {
                    System.err.println(String.format("Warning: Unknown argument<line: %d>: '%s'", cnt, paras[0]));
                }
            }
        }
        catch (FileNotFoundException e)
        {
            System.err.println("Warning: Configuration file not found: '" + path + "'. Using default configurations.");
            System.err.println("Warning: Creating empty configuration file.");
            File f = new File(path);
            try
            {
                f.createNewFile();
            }
            catch (IOException ee)
            {
                System.err.println("Error: Configuration file creation failed.");
            }
        }
        catch (IOException e)
        {
            System.err.println("Error: Configuration file reading error. Abort.");
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            System.err.println(String.format("Error: Broken line in configuration file<line: %d>", cnt));
        }
    }*/

    /*public String get(String var)
    {
        return pool.get(var);
    }*/

    /* Show all paras */
    /*public void show()
    {
        Field fields[] = Config.class.getFields();
        int mask = Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL;
        for (Field f : fields)
        {
            int modifier = f.getModifiers();
            if (mask == (mask & modifier))
            {
                System.out.println(String.format("<%s = %s>", f.getName(), pool.get(f.getName())));
            }
        }
    }*/
}
