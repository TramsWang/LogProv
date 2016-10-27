package com.logprov;

import com.logprov.pipeline.PipelineMonitor;

import java.io.IOException;

/**
 * Created by babyfish on 16-10-26.
 */
public class LogProv {

    private static Config config;
    private static PipelineMonitor pm;

    public static void main(String args[]) throws Exception
    {
        config = new Config();
        pm = new PipelineMonitor();
        pm.initiate();
    }
}
