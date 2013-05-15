package com.sap.hadoop.js;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.task.ITask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/29/12
 * Time: 3:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class MrRunner implements ITask {

    private static String INPUT_PATH;
    private static String OUTPUT_PATH;

    public MrRunner(String inPath, String outPath) {
        INPUT_PATH = inPath;
        OUTPUT_PATH = outPath;
    }

    /**
     * The reduce class
     */
    public static class JsReducer extends ReducerBase<Text, Text, Text, Text> {
    }

    /**
     * The map class, the content of the log is fed as line number (KEY) and line content (VALUE)
     */
    public static class JsMapper extends MapperBase<Text, Text, Text, Text> {
    }

    public Job getMapReduceJob() throws Exception {
        final ConfigurationManager configurationManager = new ConfigurationManager("lroot", "abcd1234");
        final Configuration conf = configurationManager.getConfiguration();

        conf.set("js.reduce.output.key.type", "LongWritable");
        conf.set("js.reduce.output.value.type", "Text");
        conf.set("js.reduce.filename", "/user/lroot/ds/jsMr/mapreduce/map.js");

        conf.set("js.map.output.key.type", "Text");
        conf.set("js.map.output.value.type", "IntWritable");
        conf.set("js.map.filename", "/user/lroot/ds/jsMr/mapreduce/reduce.js");

        Job job = new Job(conf, "MrRunner for " + INPUT_PATH);

        job.setJarByClass(MrRunner.class);

        job.setMapperClass(JsMapper.class);
        job.setReducerClass(JsReducer.class);

        // Read from a folder of log access
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        // Write to a folder with many files
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        // Map's outputs
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce's outputs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    public static void main(String[] arg) throws Exception {
        MrRunner mrRunner = new MrRunner(arg[0], arg[1]);
        mrRunner.getMapReduceJob().waitForCompletion(true);
    }

}
