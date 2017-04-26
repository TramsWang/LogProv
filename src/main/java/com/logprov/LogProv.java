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
 * TODO: Add function about 'Statistical Distance(Divergence)'
 * TODO: Write new usage example
 * TODO: Modify comments of every class
 */
public class LogProv {

    public static void main(String args[]) throws Exception
    {
        PipelineMonitor pm = new PipelineMonitor();
        pm.initiate();
    }
}
