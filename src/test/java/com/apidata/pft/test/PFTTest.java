package com.apidata.pft.test;

import com.apidata.pft.PFTClient;
import com.apidata.pft.PFTConstants;
import com.apidata.pft.PFTServer;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class PFTTest extends AbstractTest {
    private static final String FILE_1 = "/src/test/resource/file_data.txt";
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 54231;
    private String clientPath;

    @Before
    public void setUp() throws IOException {
        srcFile = new File(System.getProperty("user.dir") + FILE_1);
        long timestamp = System.currentTimeMillis();
        clientPath = "/tmp" + "/" + srcFile.getName();
        targetFile = new File(clientPath);
    }

    @Test
    public void downloadFile() throws InterruptedException {

        // Start PFTServer
        PFTServer pftServer = new PFTServer(HOSTNAME, PORT);

        Thread thread = new Thread() {
            public void run() {
                pftServer.doWork();
            }
        };
        thread.start();

        Thread.sleep(3000);

        // Start PFTClient
        PFTClient pftClient = new PFTClient(HOSTNAME, PORT, srcFile.getAbsolutePath(), clientPath,
                PFTConstants.MAX_BUFFER_PER_THREAD);
        pftClient.doWork();

        compare();
        targetFile.delete();
        thread.interrupt();
    }
}
