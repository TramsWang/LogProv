package com.logprov.sampleUDF;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.Random;

/**
 * @author  Ruoyu Wang
 * @version 1.0
 * Date:    2017.05.01
 *
 *   This evaluation function is used in illustration examples. It generates a random integer sequence. The tuple it
 * accepts should contain three files:
 *   $0: random origin
 *   $1: random boundary
 *   $2: random sequence length
 */
public class RandomGenerate extends EvalFunc<DataBag> {

    private Random random = new Random();

    public RandomGenerate(){}

    public DataBag exec(Tuple t) throws ExecException
    {
        int origin = (Integer)t.get(0);
        int boundary = (Integer)t.get(1);
        int length = (Integer)t.get(2);

        DataBag bag = BagFactory.getInstance().newDefaultBag();

        for (int i = 0; i < length; i++)
        {
            Tuple tmp = TupleFactory.getInstance().newTuple();
            tmp.append(random.nextInt(boundary - origin) + origin);
            bag.add(tmp);
        }

        return bag;
    }
}
