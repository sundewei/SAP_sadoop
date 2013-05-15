package com.sap.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 18, 2011
 * Time: 2:36:54 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ConfigurationManager {

    private DFSImpl fileSystem;

    private static String DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";

    private static final Logger LOG = Logger.getLogger(ConfigurationManager.class.getName());

    public static final String CORE_SITE_XML = "core-site.xml";
    public static final String CORE_SITE_DEFAULT_XML = "core-default.xml";

    public static final String HDFS_SITE_XML = "hdfs-site.xml";
    public static final String HDFS_SITE_DEFAULT_XML = "hdfs-default.xml";

    public static final String MAPRED_SITE_XML = "mapred-site.xml";
    public static final String MAPRED_SITE_DEFAULT_XML = "mapred-default.xml";

    public static final String HBASE_SITE_XML = "hbase-site.xml";

    public static final String SAP_XML = "sap.xml";

    private static String CONF_URL;

    private Configuration configuration;

    private static Configuration SAVED_CONFIGURATION;
    private static long SAVED_TIME;

    private boolean isInited = false;
    private String username;

    private URL getResource(String name) throws IOException {
        if (name.equals(SAP_XML)) {
            LOG.info("Adding resource as " + name);
            return ConfigurationManager.class.getResource(name);
        }

        if (CONF_URL == null) {
            CONF_URL =
                    getConfiguration().get("com.sap.hadoop.conf.ConfigurationManager.confUrl");
        }
        String urlAddress = CONF_URL + name;
        LOG.info("Adding resource as " + urlAddress);
        URL url = new URL(urlAddress);
        //System.out.println("The URL to add...: " + url.toExternalForm());
        return url;
    }

    public Connection getConnection() throws SQLException {
        readyToGo();
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            LOG.error(e);
            throw new SQLException(e);
        }
        return DriverManager.getConnection("jdbc:hive://" + getNameNode() + ":10000/default", "", "");
    }

    public ConfigurationManager(String username, String password) {
        this.username = username;
        init(username, password);
    }

    public String getUsername() {
        return username;
    }

    public String getRemoteFolder() {
        return "/user/" + getUsername() + "/";
    }

    public Configuration getConfiguration() {
        return new Configuration(configuration);
    }

    private void init(String username, String password) {
        configuration = getSavedConfiguration();
        try {
            if (configuration == null) {
                LOG.info("About to load config xml files...");
                configuration = new Configuration();
                configuration.addResource(getResource(SAP_XML));
                configuration.addResource(getResource(CORE_SITE_DEFAULT_XML));
                configuration.addResource(getResource(HDFS_SITE_DEFAULT_XML));
                configuration.addResource(getResource(MAPRED_SITE_DEFAULT_XML));
                configuration.addResource(getResource(CORE_SITE_XML));
                configuration.addResource(getResource(HDFS_SITE_XML));
                configuration.addResource(getResource(MAPRED_SITE_XML));
                configuration.addResource(getResource(HBASE_SITE_XML));
                LOG.info("Done loading xml files...now added the ugi property manually");
                configuration.set("hadoop.job.ugi", username + ", " + password);
                LOG.info("Done loading config xml files, saving it now...");
                saveConfiguration(configuration);
            } else {
                LOG.info("Using saved Configuration loaded at " + new Timestamp(SAVED_TIME));
            }
            isInited = true;
        } catch (IOException ioe) {
            LOG.error(ioe);
            RuntimeException re = new RuntimeException(ioe);
            re.setStackTrace(ioe.getStackTrace());
            throw re;
        }

        configuration.reloadConfiguration();
    }

    private synchronized static Configuration getSavedConfiguration() {
        // One hour
        if (SAVED_CONFIGURATION != null && System.currentTimeMillis() - SAVED_TIME < 3600000) {
            return SAVED_CONFIGURATION;
        } else {
            return null;
        }
    }

    private synchronized static void saveConfiguration(Configuration configuration) {
        SAVED_CONFIGURATION = configuration;
        SAVED_TIME = System.currentTimeMillis();
    }

    private String getNameNode() {
        String nameNode = configuration.get("fs.default.name");
        String prefix = "hdfs://";
        int prefixIdx = nameNode.indexOf(prefix);
        return nameNode.substring(prefixIdx + prefix.length(), nameNode.lastIndexOf(":"));
    }

    private void readyToGo() {
        if (!isInited) {
            throw new RuntimeException("ConfigurationManager has not been initialized!");
        }
    }

    public IFileSystem getFileSystem() throws Exception {
        readyToGo();
        if (fileSystem == null) {
            fileSystem = new DFSImpl(this);
        }
        return fileSystem;
    }

    public static void main(String[] args) throws Exception {

        ConfigurationManager cm = new ConfigurationManager("lroot", "abcd1234");

        // Get the Hadoop Configuration object
        Configuration configuration = cm.getConfiguration();

        // Get the remote working folder for the user you logged in
        String remoteWorkingFolder = cm.getRemoteFolder();



        IFileSystem fileSystem = cm.getFileSystem();
        boolean uploadOk =
                fileSystem.uploadFromLocalFileWithRetry(cm.getRemoteFolder() + "insertSQL.txt",
                        "C:\\projects\\data\\insertSQL.txt", 3);
        System.out.println("uploadOk=" + uploadOk);
        System.exit(0);
        /*
        IOperationContext priorityContext = ContextFactory.createPriorityContext(cm);
        OperationBase operation =
                new AsynCopyFileOperation(cm, "C:\\data\\amazonAccess\\amzn-anon-access-samples-2.0.csv",
                        cm.getRemoteFolder() + "amzn-anon-access-samples-2.0.csv");
        priorityContext.addOperation(operation);
        priorityContext.runOperations();

        CopyFileStatus status = (CopyFileStatus)priorityContext.getStatus(operation.getOperationId());

        while (true) {
            System.out.println("Sleeping 5 seconds...");
            Thread.sleep(5000);
            System.out.println(status);
            if (status != null && status.getEndMs() > 0) {
System.out.println("Exiting condition 0");
System.out.println("status.getEndMs()="+status.getEndMs());
                break;
            }

            if (status.isFailedWithError()) {
System.out.println("Exiting condition 1");
                status.getException().printStackTrace();
                break;
            }
        }

        */
        //cm.getFileSystem().uploadFromLocalFile(cm.getRemoteFolder() + "amzn-anon-access-samples-2.0.csv", "C:\\data\\amazonAccess\\amzn-anon-access-samples-2.0.csv");
    }
}
