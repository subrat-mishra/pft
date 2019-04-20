package com.apidata.pft;

import com.apidata.pft.exception.SocketCloseException;
import com.apidata.pft.message.FileRequestMsg;
import com.apidata.pft.message.FileResponseMsg;
import com.apidata.pft.message.Message;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;

import static com.apidata.pft.PFTConstants.BUFFER_SIZE;

/**
 * PFTClient opens a SocketChannel to the server running on hostName and port configured.
 * First Client sends a FileRequestMsg to the server and get the FileResponseMsg. Next based on
 * the FileResponseMsg it creates multiple {@link PFTChunkClient} threads which reads the data from
 * server using SocketChannel. {@link FileMerger} thread merges the all the chunked files data into
 * one file as soon as {@link PFTChunkClient} thread is completed in sequence of chunkId.
 */
public class PFTClient {
    private static final Logger LOG = LoggerFactory.getLogger(PFTClient.class);
    private static final int EXECUTORS = 10;
    private static final int MAX_BUFFER_PER_THREAD = 9998336;

    private String hostName;
    private int port;
    private String serverFilePath;
    private String clientFilePath;

    public PFTClient(String hostName, int port, String serverFilePath, String clientFilePath) {
        this.hostName = hostName;
        this.port = port;
        this.serverFilePath = serverFilePath;
        this.clientFilePath = clientFilePath;
    }

    public void doWork() {
        InetSocketAddress hostAddress = new InetSocketAddress(hostName, port);
        try {
            SocketChannel client = SocketChannel.open(hostAddress);
            LOG.info("Connect to server:{}", client.getRemoteAddress());

            // Step-1: Get FileSize from the server.
            long fileSize = 0;
            if (client.isOpen()) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);

                // we formulate the request message and send it to the server.
                FileRequestMsg msg = new FileRequestMsg(serverFilePath);
                Message.sendMessage(client, msg);

                // we then await the servers response.
                Message response = Message.nextMsgFromSocket(client, byteBuffer);
                FileResponseMsg fileResponseMsg = (FileResponseMsg) response;
                fileSize = fileResponseMsg.getFileSize();
                LOG.info("Response received filesize={} ", fileSize);
            }

            long total = fileSize / MAX_BUFFER_PER_THREAD;
            boolean isRemaining = fileSize % MAX_BUFFER_PER_THREAD > 0;
            if (isRemaining) {
                total++;
            }
            PriorityBlockingQueue<Integer> queue = new PriorityBlockingQueue<>();
            CountDownLatch latch = new CountDownLatch(1);

            //Step-2 Start a thread to complete the fileMerger as soon as chunk has come.
            FileMerger merger = new FileMerger(total, queue, clientFilePath, latch);
            merger.start();

            // Step-3: Based on the fileSize decide the number of threads required
            ExecutorService executorService = Executors.newFixedThreadPool(EXECUTORS);
            if (fileSize > 0) {
                CompletionService<Result>
                        completionService =
                        new ExecutorCompletionService<>(executorService);
                List<Future> futures = new ArrayList<>();
                for (int i = 0; i < total; i++) {
                    long
                            offset =
                            (isRemaining && i == total - 1) ?
                                    (fileSize % MAX_BUFFER_PER_THREAD) :
                                    MAX_BUFFER_PER_THREAD;
                    PFTChunkClient
                            pftChunkClient =
                            new PFTChunkClient(i, hostName, port, serverFilePath,
                                    clientFilePath + PFTConstants.PART + i, offset,
                                    MAX_BUFFER_PER_THREAD);
                    futures.add(completionService.submit(pftChunkClient));
                }
                try {
                    int count = 0;
                    long startTime = System.currentTimeMillis();
                    for (int i = 0; i < total; i++) {
                        Result result = completionService.take().get();
                        LOG.info("Completed: {} in {}, progress {}/{}", result.getId(),
                                result.getTimeTaken(), count++, total);
                        if (!result.isStatus()) {
                            // TODO: can be re-tried.
                            throw new RuntimeException(
                                    "Unable to proceed as thread is not successfull-" + result
                                            .getId());
                        }
                        queue.offer(result.getId());
                    }
                    long endTime = System.currentTimeMillis();
                    LOG.info("Completed successfully in {} msecs", endTime - startTime);
                } catch (InterruptedException e) {
                    LOG.error("InterruptedException occurred", e);
                } catch (Exception e) {
                    LOG.error("Exception occurred", e);
                    merger.interrupt();
                } finally {
                    executorService.shutdown();
                }
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                LOG.error("InterruptedException occurred", e);
            }

            client.close();
        } catch (IOException e) {
            LOG.error("IOException occurred", e);
        } catch (SocketCloseException e) {
            LOG.error("SocketCloseException occurred", e);
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("pft-client", options);
    }

    private static Options generateOptions() {
        Options options = new Options();
        options.addOption("H", "hostname", true, "HostName of the server, Default=localhost");
        options.addOption("P", "port", true, "Server port number, Default=54321");
        options.addOption("S", "serverFilePath", true, "Server File to be downloaded");
        options.addOption("C", "clientFilePath", true,
                "Client File to be copied, Default=/tmp/<epochTime>/<server-file>");
        options.addOption("h", "help", false, "Help usage");
        return options;
    }

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        final Options options = generateOptions();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                printUsage(options);
                System.exit(1);
            }
            String hostName = cmd.getOptionValue("H", "localhost");
            int port = Integer.parseInt(cmd.getOptionValue("P", "54321"));
            String serverFilePath = cmd.getOptionValue("S");
            if (serverFilePath == null) {
                printUsage(options);
                System.exit(1);
            }

            long currentTs = System.currentTimeMillis();
            String clientFilePath = cmd.getOptionValue("C");
            if (clientFilePath == null) {
                String fileName = serverFilePath.substring(serverFilePath.lastIndexOf("/"));
                clientFilePath = "/tmp/" + currentTs + "/" + fileName;
            }
            File file = new File(clientFilePath);
            File parentFile = file.getParentFile();
            if (!parentFile.exists()) {
                parentFile.mkdirs();
            }
            if (file.exists()) file.delete();

            PFTClient pftClient = new PFTClient(hostName, port, serverFilePath, clientFilePath);
            pftClient.doWork();

        } catch (ParseException e) {
            LOG.error("Parsing error occurred", e);
            printUsage(options);
        }
    }
}

/**
 * FileMerger thread waits for PriorityBlockingQueue to get sequence of chunkId file to be completed
 * and then merge to one.
 */
class FileMerger extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(FileMerger.class);

    private int lastCount = 0;
    private long totalCount;
    private String clientFilePath;
    private PriorityBlockingQueue<Integer> queue;
    private CountDownLatch latch;

    public FileMerger(long totalCount, PriorityBlockingQueue<Integer> queue, String clientFilePath,
            CountDownLatch latch) {
        this.totalCount = totalCount;
        this.queue = queue;
        this.clientFilePath = clientFilePath;
        this.latch = latch;
    }

    @Override
    public void run() {
        FileWriter fstream = null;
        BufferedWriter out = null;
        try {
            File clientFile = new File(clientFilePath);
            if (clientFile.exists()) clientFile.delete();

            fstream = new FileWriter(clientFile, true);
            out = new BufferedWriter(fstream);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        if (out == null) {
            throw new RuntimeException("Unable to create a BufferedWriter for " + clientFilePath);
        }
        long totalBytesCopied = 0;
        try {
            while (!queue.isEmpty() || lastCount < totalCount) {
                int value = queue.take();
                if (value == lastCount) {
                    // Merge File
                    FileInputStream fis;
                    try {
                        File f = new File(clientFilePath + PFTConstants.PART + value);
                        fis = new FileInputStream(f);
                        BufferedReader in = new BufferedReader(new InputStreamReader(fis));

                        char[] buf = new char[BUFFER_SIZE];
                        int read;
                        long fileBytes = 0;
                        while ((read = in.read(buf)) != -1) {
                            out.write(buf, 0, read);
                            fileBytes += read;
                        }

                        in.close();
                        fis.close();
                        f.delete();
                        totalBytesCopied += fileBytes;
                        LOG.info("Merge successfully file-{} {} {}", value, fileBytes,
                                totalBytesCopied);
                    } catch (IOException e) {
                        LOG.error("IOException occurred", e);
                    }
                    lastCount++;
                } else {
                    queue.offer(value);

                    // Wait for few millisecs.
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        LOG.error("InterruptedException occurred", e);
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.error("InterruptedException occurred", e);
        }

        try {
            out.close();
            fstream.close();
        } catch (IOException e) {
            LOG.error("IOException occurred", e);
        }
        if (lastCount == totalCount) {
            LOG.info("Successfully merged file {}", clientFilePath);
        } else {
            LOG.error("Error occurred unable to merge files {}/{}", lastCount, totalCount);
        }

        latch.countDown();
    }
}