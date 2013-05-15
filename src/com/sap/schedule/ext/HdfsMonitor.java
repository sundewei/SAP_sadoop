package com.sap.schedule.ext;

import com.sap.schedule.IMonitor;

import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/25/13
 * Time: 2:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsMonitor implements IMonitor {
    private String rootHdfsDirName;

    public HdfsMonitor () {
    }

    public void configure(Properties properties) {
        rootHdfsDirName = properties.getProperty("rootHdfsDirName");
    }

    public IMonitor getMonitor() {

    }


}
