package com.logprov.sampleUDF;

import org.apache.pig.EvalFunc;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.util.Random;

/**
 * @author  Ruoyu Wang
 * @version 1.0
 * Date:    2017.05.01
 *
 *   This filtering function is used in illustration examples. It randomly drops records.
 */
public class Drop extends FilterFunc {

    private Random random;
    private double drop_rate;

    public Drop(String drop_rate)
    {
        random = new Random();
        this.drop_rate = Double.valueOf(drop_rate);
    }

    public Boolean exec(Tuple t)
    {
        double d = random.nextDouble();
        return (drop_rate > d);
    }
}
