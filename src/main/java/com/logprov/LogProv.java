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
 * TODO: Write new usage example
 */
public class LogProv {

    public static void main(String args[]) throws Exception
    {
        PipelineMonitor pm = new PipelineMonitor();
        //pm.test();
        pm.initiate();
    }
}
