package com.sap.schedule;

import org.apache.hadoop.mapred.TaskStatus;

import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/25/13
 * Time: 1:44 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IMonitor {
    public void configure(Properties properties);
    public boolean isFinished(int executeId);
    public long getLastUpdateTime(int executeId);
    public float getProgress(int executeId);
    public ExecutionStatus getExecutionStatus(int executeId);
}
