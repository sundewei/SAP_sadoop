package com.sap.hadoop.conf;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 24, 2011
 * Time: 11:11:40 AM
 * To change this template use File | Settings | File Templates.
 */
public interface IFileSystem {

    public boolean deleteFile(String filename) throws Exception;

    public boolean mkdirs(String foldername) throws Exception;

    public boolean deleteDirectory(String foldername) throws Exception;

    public boolean exists(String filename) throws Exception;

    public boolean uploadFromLocalFile(String remoteFilename, String localFilename) throws Exception;

    public boolean uploadFromLocalFileWithRetry(String remoteFilename, String localFilename, int retryCount) throws Exception;

    public IFile[] listFiles(String folder) throws Exception;

    public long getSize(String filename) throws Exception;

    public InputStream getInputStream(String remoteFile) throws Exception;

    public OutputStream getOutputStream(String remoteFile) throws Exception;

    public void close() throws Exception;
}
