package com.sap.hadoop.js;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 1/26/12
 * Time: 3:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class MapperBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    protected ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
    protected ScriptEngine scriptEngine;

    protected WritableComparable mapOutputKey;
    protected WritableComparable mapOutputValue;

    private static Class OUTPUT_KEY_CLASS = null;
    private static Class OUTPUT_VALUE_CLASS = null;

    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
        try {
            Configuration configuration = context.getConfiguration();
            mapOutputKey = MrUtils.getWritableComparableType("js.map.output.key.type", configuration);
            mapOutputValue = MrUtils.getWritableComparableType("js.map.output.value.type", configuration);
            scriptEngine.eval(MrUtils.getScripts(context.getConfiguration(),
                    MrUtils.getPathFilter("js.map.filename", configuration)));
            scriptEngine.put("mapOutputKey", mapOutputKey);
            scriptEngine.put("mapOutputValue", mapOutputValue);
            OUTPUT_KEY_CLASS = mapOutputKey.getClass();
            OUTPUT_VALUE_CLASS = mapOutputValue.getClass();
        } catch (ScriptException se) {
            IOException ioe = new IOException(se);
            ioe.setStackTrace(se.getStackTrace());
            throw ioe;
        }
    }

    public void map(KEYIN inKey, VALUEIN inValue, Context context) throws IOException, InterruptedException {
        try {
            callFunction("map", inKey, inValue, context);
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
