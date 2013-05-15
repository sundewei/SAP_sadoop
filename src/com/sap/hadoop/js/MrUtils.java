package com.sap.hadoop.js;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 1/30/12
 * Time: 3:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class MrUtils {
    public static PathFilter getPathFilter(final String filenameKey, final Configuration configuration) {
        final String filename = configuration.get(filenameKey);
        return new PathFilter() {
            public boolean accept(Path path) {
                if (path != null) {
                    String folder = path.getParent().getName();
                    if (!folder.endsWith("/")) {
                        folder += "/";
                    }
                    return filename.equals(folder + path.getName());
                } else {
                    return true;
                }
            }
        };
    }

    public static String getScripts(Configuration configuration, PathFilter filter) throws IOException {
        // Get all cached files
        Path[] files = DistributedCache.getLocalCacheFiles(configuration);
        StringBuilder stringBuilder = new StringBuilder();

        // Merge them into a string
        for (Path file : files) {
            // Either accept all files or the ones acceptable by the filter
            if (filter == null || filter.accept(file)) {
                FileSystem fileSystem = file.getFileSystem(configuration);
                BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(file)));
                String line = reader.readLine();
                while (line != null) {
                    stringBuilder.append(line).append("\n");
                    line = reader.readLine();
                }
                reader.close();
            }
        }
        return stringBuilder.toString();
    }

    public static WritableComparable getReduceOutputKeyType(Configuration configuration) {
        return getWritableComparableType("js.reduce.output.map.type", configuration);
    }

    public static WritableComparable getReduceOutputValueType(Configuration configuration) {
        return getWritableComparableType("js.reduce.output.value.type", configuration);
    }

    public static WritableComparable getWritableComparableType(String typeKey, Configuration configuration) {
        String type = configuration.get(typeKey, "Text");
        if (type.equalsIgnoreCase("BooleanWritable")) {
            return new Text();
        } else if (type.equalsIgnoreCase("BytesWritable")) {
            return new BytesWritable();
        } else if (type.equalsIgnoreCase("ByteWritable")) {
            return new ByteWritable();
        } else if (type.equalsIgnoreCase("DoubleWritable")) {
            return new DoubleWritable();
        } else if (type.equalsIgnoreCase("FloatWritable")) {
            return new FloatWritable();
        } else if (type.equalsIgnoreCase("IntWritable")) {
            return new IntWritable();
        } else if (type.equalsIgnoreCase("LongWritable")) {
            return new LongWritable();
        } else if (type.equalsIgnoreCase("MD5Hash")) {
            return new MD5Hash();
        } else if (type.equalsIgnoreCase("NullWritable")) {
            return NullWritable.get();
        } else if (type.equalsIgnoreCase("Text")) {
            return new Text();
        } else if (type.equalsIgnoreCase("VIntWritable")) {
            return new VIntWritable();
        } else if (type.equalsIgnoreCase("VLongWritable")) {
            return new VLongWritable();
        } else {
            // default is text
            return new Text();
        }
    }
}
