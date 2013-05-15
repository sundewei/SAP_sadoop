package com.sap.hadoop.operation;

import com.sap.hadoop.concurrent.OperationBase;
import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IStatus;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/13/11
 * Time: 2:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class AsynCopyFileOperation extends OperationBase {
    private List<Long> previousOperationIds = new LinkedList<Long>();
    private long operationId;
    private String name;
    private String localFilename;
    private String remoteFilename;
    private ConfigurationManager configurationManager;
    private IStatus status;

    // In bytes
    private long localFileLength = -1l;

    public AsynCopyFileOperation(ConfigurationManager cm, String localFilename, String remoteFilename) {
        configurationManager = cm;
        this.name = "(" + operationId + "), Copy local file: " + localFilename + " to HDFS: " + remoteFilename;
        this.localFilename = localFilename;
        this.remoteFilename = remoteFilename;
        operationId = System.currentTimeMillis();
        status = new CopyFileStatus(configurationManager, operationId, name, localFilename, remoteFilename);
        ((CopyFileStatus) status).setLocalFileLength(getLocalFileLength());
    }

    public long getOperationId() {
        return operationId;
    }

    public String getName() {
        return name;
    }

    public void setPreviousOperationId(long previousOperationId) {
        previousOperationIds.add(previousOperationId);
    }

    public long getLocalFileLength() {
        if (localFileLength < 0) {
            File file = new File(localFilename);
            localFileLength = file.length();
        }
        return localFileLength;
    }

    public IStatus getStatus() {
        return status;
    }

    public void run() {
        System.out.println("Start of run() ");
        try {
            ((CopyFileStatus) status).setEndMs(System.currentTimeMillis());
            configurationManager.getFileSystem().uploadFromLocalFile(remoteFilename, localFilename);
            ((CopyFileStatus) status).setEndMs(System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
            status.setFailedWithError();
            ((CopyFileStatus) status).setException(e);
        }

        System.out.println("End of run() ");
    }

    public OperationBase getRetryOperation(IStatus status) {
        return new AsynCopyFileOperation(configurationManager, localFilename, remoteFilename);
    }
}
