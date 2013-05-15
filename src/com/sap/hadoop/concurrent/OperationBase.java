package com.sap.hadoop.concurrent;

import com.sap.hadoop.conf.IStatus;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/13/11
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class OperationBase implements Runnable {
    public abstract long getOperationId();

    public abstract void run();

    public abstract IStatus getStatus();

    public abstract OperationBase getRetryOperation(IStatus status);

    public abstract void setPreviousOperationId(long pid);
}
