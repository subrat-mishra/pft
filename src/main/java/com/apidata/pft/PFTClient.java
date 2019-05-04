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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.apidata.pft.PFTConstants.BUFFER_SIZE;
import static com.apidata.pft.PFTConstants.MAX_BUFFER_PER_THREAD;

/**
 * PFTClient opens a SocketChannel to the server running on hostName and port configured.
 * First Client sends a FileRequestMsg to the server and get the FileResponseMsg. Next based on
 * the FileResponseMsg it creates multiple {@link PFTChunkClient} threads which reads the data from
 * server using SocketChannel and writes to a RandomAccessFile with the required offset position.
 */
public class PFTClient {
    private static final Logger LOG = LoggerFactory.getLogger(PFTClient.class);
    private static final int EXECUTORS = 10;

    private String hostName;
    private int port;
    private String serverFilePath;
    private String clientFilePath;
    private int maxBufferPerThread;

    public PFTClient(String hostName, int port, String serverFilePath, String clientFilePath,
            int maxBufferPerThread) {
        this.hostName = hostName;
        this.port = port;
        this.serverFilePath = serverFilePath;
        this.clientFilePath = clientFilePath;
        this.maxBufferPerThread = maxBufferPerThread;
    }

    public void doWork() {
        InetSocketAddress hostAddress = new InetSocketAddress(hostName, port);
        RandomAccessFile clientFile = null;
        SocketChannel client = null;
        try {
            client = SocketChannel.open(hostAddress);
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
            client.close();

            long total = fileSize / maxBufferPerThread;
            boolean isRemaining = fileSize % maxBufferPerThread > 0;
            if (isRemaining) {
                total++;
            }

            // Created a RandomAccessFile for clientFile
            clientFile = new RandomAccessFile(clientFilePath, "rw");

            // Step-2: Based on the fileSize decide the number of threads required
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
                                    (fileSize % maxBufferPerThread) :
                                    maxBufferPerThread;
                    PFTChunkClient
                            pftChunkClient =
                            new PFTChunkClient(i, hostName, port, serverFilePath, offset,
                                    maxBufferPerThread, clientFile.getChannel());
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
                    }
                    long endTime = System.currentTimeMillis();
                    LOG.info("Completed successfully in {} msecs", endTime - startTime);
                } catch (InterruptedException e) {
                    LOG.error("InterruptedException occurred", e);
                } catch (Exception e) {
                    LOG.error("Exception occurred", e);
                } finally {
                    executorService.shutdown();
                }
            }
            LOG.debug("Successfully created file: " + clientFilePath);
        } catch (IOException e) {
            LOG.error("IOException occurred", e);
        } catch (SocketCloseException e) {
            LOG.error("SocketCloseException occurred", e);
        } finally {
            try {
                if (clientFile != null) {
                    clientFile.close();
                }
                if (client != null) {
                    client.close();
                }
            } catch (IOException e) {
                LOG.error("IOException occurred", e);
            }
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
        options.addOption("O", "offset", true,
                "Max offset per thread, Default=" + MAX_BUFFER_PER_THREAD);
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

            int
                    maxBufferPerThread =
                    Integer.parseInt(cmd.getOptionValue("O", MAX_BUFFER_PER_THREAD + ""));

            PFTClient
                    pftClient =
                    new PFTClient(hostName, port, serverFilePath, clientFilePath,
                            maxBufferPerThread);
            pftClient.doWork();

        } catch (ParseException e) {
            LOG.error("Parsing error occurred", e);
            printUsage(options);
        }
    }
}