package com.sap.hadoop.concurrent;

import com.sap.hadoop.conf.IStatus;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/13/11
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IOperationContext {
    void runOperations();

    void addOperation(OperationBase operationBase);

    IStatus getStatus(long id);
}
