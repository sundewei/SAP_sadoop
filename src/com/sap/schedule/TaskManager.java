package com.sap.schedule;

import com.sap.schedule.ext.HdfsMonitor;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/25/13
 * Time: 1:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class TaskManager {

    private static final TaskManager INSTANCE = new TaskManager();

    IMonitor monitor = null;

    private TaskManager() {
        monitor = getMonitor();
    }

    public static TaskManager getInstance() {
        return INSTANCE;
    }

    private IMonitor getMonitor() {
        return new HdfsMonitor("/taskMamager/users/hadoop/");
    }
}
