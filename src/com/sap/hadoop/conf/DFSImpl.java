package com.sap.hadoop.conf;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/4/11
 * Time: 10:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class DFSImpl implements IFileSystem {

    private FileSystem fileSystem;

    private Configuration hadoopConfiguration;

    private ConfigurationManager configurationManager;

    private String ownerName;

    private String password;

    protected static final FsPermission FS_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.NONE);

    private static final Logger LOG = Logger.getLogger(IFileSystem.class.getName());

    DFSImpl(ConfigurationManager manager) throws Exception {
        configurationManager = manager;
        hadoopConfiguration = manager.getConfiguration();
        String ugiString = hadoopConfiguration.get("hadoop.job.ugi");
        if (ugiString == null || ugiString.length() == 0 || ugiString.split(",").length != 2) {
            throw new Exception("No \"hadoop.job.ugi\" defined, unable to get IFileSystem object.");
        }
        String[] ugiArray = ugiString.split(",");
        ownerName = ugiArray[0];
        password = ugiArray[1];
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(ownerName);
        ugi.doAs(
                new PrivilegedExceptionAction<Void>() {
                    public Void run() throws Exception {
                        //OR access hdfs
                        fileSystem = FileSystem.get(hadoopConfiguration);
                        return null;
                    }
                }
        );
    }

    public void close() throws IOException {
        fileSystem.close();
    }

    public boolean deleteFile(String filename) throws IOException {
        return fileSystem.delete(new Path(filename), true);
    }

    public boolean mkdirs(String foldername) throws IOException {
        Path folder = new Path(foldername);
        if (fileSystem.exists(folder)) {
            return true;
        } else {
            boolean createOk = fileSystem.mkdirs(folder, FS_PERMISSION);
            if (ownerName != null) {
                fileSystem.setOwner(folder, ownerName, null);
            }
            return createOk;
        }
    }

    public boolean deleteDirectory(String foldername) throws IOException {
        Path folder = new Path(foldername);
        return fileSystem.delete(folder, true);
    }

    public boolean exists(String filename) throws IOException {
        return fileSystem.exists(new Path(filename));
    }

    public boolean uploadFromLocalFile(String remoteFilename, String localFilename) {
        boolean uploadOk = true;
        try {
            fileSystem.copyFromLocalFile(new Path(localFilename), new Path(remoteFilename));
        } catch (IOException ioe) {
            uploadOk = false;
            LOG.warn(ioe);
        } catch (Exception e) {
            uploadOk = false;
            LOG.warn(e);
        }
        return uploadOk;
    }

    public boolean uploadFromLocalFileWithRetry(String remoteFilename, String localFilename, int retryCount) {
        int tryIndex = 1;
        boolean uploadOk = true;
        while (tryIndex <= retryCount) {
            boolean gotException = false;
            OutputStream out = null;
            InputStream in = null;
            try {
                // Open the streams and copy them
                out = getOutputStream(remoteFilename);
                in = new FileInputStream(localFilename);
                IOUtils.copyLarge(in, out);
            } catch (IOException ioe) {
                LOG.warn("At " + tryIndex + " try, IO error message = " + ioe.getMessage());
                gotException = true;
            } catch (Exception e) {
                LOG.warn("At " + tryIndex + " try, error message = " + e.getMessage());
                gotException = true;
            } finally {
                // Close any streams if they are open
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException ioee) {
                        LOG.warn(ioee);
                    }
                }
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException ee) {
                        LOG.warn(ee);
                    }
                }
            }

            // Log the exceptions
            if (!gotException) {
                LOG.info("At " + tryIndex + "th try, " + localFilename + " uploaded successful to HDFS as " + remoteFilename);
                break;
            }
            // Log the exception and report that retryCount has been reached
            if (gotException && tryIndex == retryCount) {
                LOG.warn("At " + tryIndex + "th try, " + localFilename + " failed to upload to HDFS as " + remoteFilename + " with error ");
                uploadOk = false;
            }
            tryIndex++;

            // Sleep 3 second before next try
            sleepQuite(3000);
        }
        return uploadOk;
    }

    private void sleepQuite(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            LOG.warn(ie);
        }
    }

    public IFile[] listFiles(String folder) throws IOException {
        FileStatus[] fss = fileSystem.listStatus(new Path(folder));
        TreeSet<IFile> files = new TreeSet<IFile>();
        TreeSet<IFile> folders = new TreeSet<IFile>();
        if (fss != null) {
            for (FileStatus fs : fss) {
                IFile file = new FileImpl(fs.getPath().getName(),
                        fs.getOwner(),
                        fs.getModificationTime(),
                        fs.getLen(),
                        fs.getPath().toUri().toASCIIString());
                if (fs.isDir()) {
                    ((FileImpl) file).setDir(true);
                    folders.add(file);
                } else {
                    files.add(file);
                }
            }
        } else {
            throw new IOException("Folder '" + folder + "' not found on HDFS");
        }

        List<IFile> content = new ArrayList<IFile>();
        content.addAll(folders);
        content.addAll(files);
        return content.toArray(new IFile[files.size()]);
    }

    public long getSize(String filename) throws IOException {
        return fileSystem.getFileStatus(new Path(filename)).getLen();
    }

    public InputStream getInputStream(String remoteFile) throws IOException {
        return fileSystem.open(new Path(remoteFile));
    }

    public OutputStream getOutputStream(String remoteFile) throws IOException {
        return fileSystem.create(new Path(remoteFile));
    }

    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("hadoop", "hadoop");
        DFSImpl dfs = new DFSImpl(cm);
        //dfs.uploadFromLocalFile(cm.getRemoteFolder() + "test.csv", "C:\\data\\amazonAccess\\amzn-anon-access-samples-2.0.csv");
        System.out.println(dfs.fileSystem.getDefaultBlockSize());
        System.out.println(dfs.fileSystem.getDefaultReplication());
        //dfs.fileSystem.copyFromLocalFile(new Path("C:\\data\\amazonAccess\\amzn-anon-access-samples-2.0.csv"), new Path(cm.getRemoteFolder() + "test.csv"));

        OutputStream out = dfs.fileSystem.create(new Path(cm.getRemoteFolder() + "friends.csv"));
        IOUtils.copyLarge(new FileInputStream("C:\\data\\friends.csv"), out);
        //out.write("Test data".getBytes());
        out.close();
    }
}
