package com.logprov;

import com.logprov.pipeline.PipelineMonitor;

import java.io.IOException;

/**
 * Created by:  Ruoyu Wang
 * Date:        2017.04.05
 *
 *   Main function of LogProv system.
 *
 * TODO: Write a sample case.
 * TODO: Initiate 'Config' inside PipelineMonitor
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
