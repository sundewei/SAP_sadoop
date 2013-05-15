package com.sap.hadoop.js;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 1/27/12
 * Time: 6:09 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class ReducerBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    protected ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
    protected ScriptEngine scriptEngine;

    protected WritableComparable reduceOutputKey;
    protected WritableComparable reduceOutputValue;

    private static Class OUTPUT_KEY_CLASS = null;
    private static Class OUTPUT_VALUE_CLASS = null;

    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        super.setup(context);
        scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
        try {
            Configuration configuration = context.getConfiguration();
            reduceOutputKey = MrUtils.getWritableComparableType("js.reduce.output.key.type", configuration);
            reduceOutputValue = MrUtils.getWritableComparableType("js.reduce.output.value.type", configuration);
            scriptEngine.eval(MrUtils.getScripts(context.getConfiguration(),
                    MrUtils.getPathFilter("js.reduce.filename", configuration)));
            scriptEngine.put("reduceOutputKey", reduceOutputKey);
            scriptEngine.put("reduceOutputValue", reduceOutputValue);

            OUTPUT_KEY_CLASS = reduceOutputKey.getClass();
            OUTPUT_VALUE_CLASS = reduceOutputValue.getClass();
        } catch (ScriptException se) {
            IOException ioe = new IOException(se);
            ioe.setStackTrace(se.getStackTrace());
            throw ioe;
        }
    }

    public void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
        try {
            callFunction("reduce", key, values, context);
        } catch (Exception e) {
            IOException ioe = new IOException(e);
            ioe.setStackTrace(e.getStackTrace());
            throw ioe;
        }
    }

    private Object callFunction(String functionName, Object... args) throws Exception {
        return ((Invocable) scriptEngine).invokeFunction(functionName, args);
    }

    public static Class getOutputKeyClass() {
        return OUTPUT_KEY_CLASS;
    }

    public static Class getOutputValueClass() {
        return OUTPUT_VALUE_CLASS;
    }
}
