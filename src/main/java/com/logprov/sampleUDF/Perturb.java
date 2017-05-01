package com.logprov.sampleUDF;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import java.util.Random;

/**
 * @author  Ruoyu Wang
 * @version 1.0
 * Date:    2017.05.01
 *
 *   This evaluation function is used for illustration examples. It adopt an amplification on original integer.
 */
public class Perturb extends EvalFunc<Integer> {

    private double amplitude;

    public Perturb(String amplitude)
    {
        this.amplitude = Double.valueOf(amplitude);
    }

    public Integer exec(Tuple t) throws ExecException
    {
        int i = (Integer)t.get(0);
        return (int)(i * (1 + amplitude));
    }
}
