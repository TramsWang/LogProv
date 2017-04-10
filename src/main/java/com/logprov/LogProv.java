package com.logprov;

import com.logprov.pipeline.PipelineMonitor;

import java.io.IOException;

/**
 * @author  Ruoyu Wang
 * @version 1.1
 * Date:    2017.04.10
 *
 *   Main function of LogProv system.
 *
 * TODO: Write a sample case.
 */
public class LogProv {

    //private static Config config;
    //private static PipelineMonitor pm;

    public static void main(String args[]) throws Exception
    {
        //config = new Config();
        PipelineMonitor pm = new PipelineMonitor();
        pm.initiate();
    }
}
