package com.sap.hadoop.concurrent;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IStatus;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/13/11
 * Time: 3:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class PriorityContext implements IOperationContext {

    private static final Map<Long, IStatus> STATUS_MAP = new HashMap<Long, IStatus>();

    Collection<OperationBase> operations = new ArrayList<OperationBase>();

    private static final Logger LOG = Logger.getLogger(PriorityContext.class.getName());

    private static ConfigurationManager configurationManager;

    private ExecutorService executor;

    private static PriorityContext INSTANCE;

    private PriorityContext() {
        executor = Executors.newCachedThreadPool();
    }

    static IOperationContext getInstance(ConfigurationManager cm) {
        if (INSTANCE == null) {
            INSTANCE = new PriorityContext();
        }
        configurationManager = cm;
        return INSTANCE;
    }

    private static void addOperation(long id, IStatus status) {
        STATUS_MAP.put(id, status);
    }

    public void addOperation(OperationBase operationBase) {
        operations.add(operationBase);
    }

    public void runOperations() {
        for (OperationBase ob : operations) {
            // Run the operation right away
            addOperation(ob.getOperationId(), ob.getStatus());
            executor.submit(ob);
        }
    }

    public IStatus getStatus(long id) {
        return STATUS_MAP.get(id);
    }
}
