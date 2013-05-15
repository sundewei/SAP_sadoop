package com.sap.hadoop.conf;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/13/11
 * Time: 11:58 AM
 * To change this template use File | Settings | File Templates.
 */
public interface IStatus {
    public long getStartMs();

    public long getEndMs();

    public String getOperationName();

    public float getProgress() throws Exception;

    public void setFailedWithError();

    public boolean isFailedWithError();
}
