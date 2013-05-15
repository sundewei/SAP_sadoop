package com.sap.hadoop.operation;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IStatus;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/13/11
 * Time: 12:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class CopyFileStatus implements IStatus {
    public ConfigurationManager cm;
    private long operationId;
    private String localFilename;
    private String hdfsFilename;
    private long localFileLength;
    private String name;
    private boolean failedWithError = false;
    private Exception exception;

    private long startMs;
    private long endMs = -1l;

    public CopyFileStatus(ConfigurationManager cm, long id, String name, String localFilename, String hdfsFilename) {
        this.cm = cm;
        this.operationId = id;
        this.localFilename = localFilename;
        this.hdfsFilename = hdfsFilename;
        this.name = name;
        startMs = System.currentTimeMillis();
    }

    public void setLocalFileLength(long localFileLength) {
        this.localFileLength = localFileLength;
    }

    public long getLocalFileLength() {
        return localFileLength;
    }

    public long getCopiedLength() throws Exception {
        if (cm.getFileSystem().exists(hdfsFilename)) {
            return cm.getFileSystem().getSize(hdfsFilename);
        }
        return 0l;
    }

    public String getOperationName() {
        return name;
    }

    public float getProgress() throws Exception {
        return ((float) getCopiedLength()) / ((float) getLocalFileLength());
    }

    public long getStartMs() {
        return startMs;
    }

    public long getEndMs() {
        return endMs;
    }

    public void setEndMs(long endMs) {
        this.endMs = endMs;
    }

    @Override
    public String toString() {
        String to = "Unable to get string representation for status: " + name;
        try {
            to =
                    "CopyFileStatus{" +
                            "  name='" + name + "'\n" +
                            ", operationId=" + operationId + "'\n" +
                            ", localFilename='" + localFilename + "'\n" +
                            ", hdfsFilename='" + hdfsFilename + "'\n" +
                            ", localFileLength=" + localFileLength + "'\n" +
                            ", copiedLength=" + getCopiedLength() + "'\n" +
                            ", progress=" + getProgress() + "'\n" +
                            ", startMs=" + startMs + "'\n" +
                            ", endMs=" + endMs + "',\n" +
                            ", failedWithError=" + failedWithError + "',\n" +
                            ", exception=" + exception + "'\n" +
                            "}\n";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return to;
    }

    public void setFailedWithError() {
        failedWithError = true;
    }

    public boolean isFailedWithError() {
        return failedWithError;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
}
