package co.cask.cdap.examples.longrunner;

import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.security.authentication.client.AccessToken;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Client to send data
 */
public class Ingestor {
  private static final Logger LOG = LoggerFactory.getLogger(Ingestor.class);

  private enum Action {
    SEND("Send Stream Events data. parameters --events and --payload"),
    HELP("Show this help.");

    private final String description;
    private Action(String description) {
      this.description = description;
    }

    private String getDescription() {
      return description;
    }
  }

  public void doMain(String[] args) throws Exception {

    /**
     * Set of Action available in this tool.
     */
    System.out.println(String.format("%s", getClass().getSimpleName()));
    System.out.println();

    final IngestAction action = getAction(args);
    if (action == null) {
      return;
    }

    final CountDownLatch shutDownLatch = new CountDownLatch(1);
    // shutdown thread
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          action.stop();
          shutDownLatch.countDown();
        } catch (Throwable e) {
          LOG.error("Failed to upgrade", e);
        }
      }
    });

    action.send();
    shutDownLatch.await();
    System.out.println("Shutdown complete");
  }


  public IngestAction getAction(String[] args) throws Exception {
    if (args.length < 1) {
      printHelp(false);
      return null;
    }
    Action action = parseAction(args[0]);
    if (action == null) {
      System.out.println(String.format("Unsupported action : %s", args[0]));
      printHelp(true);
      return null;
    }
    switch (action) {
      case SEND:
        return getIngestAction();
      case HELP:
        printHelp(false);
        break;
    }
    return null;
  }

  private Action parseAction(String action) {
    try {
      return Action.valueOf(action.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private IngestAction getIngestAction() throws Exception {
    // set up new properties object
    // from file "myProperties.txt"
    FileInputStream propFile = new FileInputStream( "src/data/ingest-properties");
    Properties p = new Properties(System.getProperties());
    p.load(propFile);
    // set the system properties
    System.setProperties(p);

    String events = System.getProperty("events");
    String payload = System.getProperty("payload");
    String streamName = System.getProperty("streamName");
    String instanceUri = System.getProperty("instanceUri", "localhost:10000");
    String accessToken = System.getProperty("accessToken", "");
    String filePath = System.getProperty("timeStampFilePath");
    String concurrency = System.getProperty("concurrency", "0");

    if (events == null || payload == null || streamName == null || filePath == null) {
      throw new Exception("Events or payload size is not available");
    }
    return new IngestAction(Integer.parseInt(events), Integer.parseInt(payload), streamName,
                            instanceUri, accessToken, Integer.parseInt(concurrency), filePath);
  }

  private void printHelp(boolean beginNewLine) {
    if (beginNewLine) {
      System.out.println();
    }
    System.out.println("Available actions: ");
    System.out.println();

    for (Action action : Action.values()) {
      System.out.println(String.format("  %s - %s", action.name().toLowerCase(), action.getDescription()));
    }
  }

  public static void main(String args[]) throws Exception {
    new Ingestor().doMain(args);
  }

  class IngestThread extends Thread {

    int events;
    int payloadSize;
    long timeStamp;
    StreamClient streamClient;
    byte[] payload;
    String instanceUri;
    String accessToken;
    String streamId;
    String timeStampRecordFile;
    List<Thread> threadList;
    int threadId;

    public IngestThread(int events, int payloadSize, String streamName, String instanceUri, String accessToken,
                        String filePath, int threadId, long timeStamp) {
      this.events = events;
      this.payloadSize = payloadSize;
      this.instanceUri = instanceUri;
      this.streamId = streamName;
      this.accessToken = accessToken;
      this.streamClient = new StreamClient(getClientConfig());
      this.payload = new byte[payloadSize];
      this.timeStampRecordFile = filePath;
      this.threadList = Lists.newArrayList();
      this.threadId = threadId;
      // useful if thread is stopped before run method is called
      this.timeStamp = timeStamp;
    }

    @Override
    public void run() {

      System.out.println("Thread :" + threadId + "started");
      while (true) {
        StringWriter writer = new StringWriter();
        for (int i = 0; i < events; i++) {
          writer.write(String.valueOf(timeStamp + ":" + payload));
          writer.write("\n");
        }

        try {
          streamClient.sendBatch(streamId, "text/plain",
                                 ByteStreams.newInputStreamSupplier(writer.toString().getBytes(Charsets.UTF_8)));
          System.out.println("Emitted " + events + " records by thread " + threadId +
                               " at timestamp : " + timeStamp / 1000);

          timeStamp += 1000;
          while (System.currentTimeMillis() < timeStamp) {
            TimeUnit.MICROSECONDS.sleep(100);
          }
        } catch (Exception e) {
          // todo : should we break or continue processing during exception?
          break;
        }
      }
    }

    public void stopThread(BufferedWriter writer, CountDownLatch latch) throws IOException {
      System.out.println("Thread :" + threadId + "stopped");
      writer.append(String.valueOf("thread:" + threadId + ",timestamp:" + timeStamp + "\n"));
      latch.countDown();
    }

    private ClientConfig getClientConfig() {
      co.cask.cdap.client.config.ClientConfig.Builder builder = new co.cask.cdap.client.config.ClientConfig.Builder();
      if (instanceUri.isEmpty()) {
        System.out.println("Instance URI is empty");
      } else {
        builder.setConnectionConfig(InstanceURIParser.DEFAULT.parse(
          URI.create(instanceUri).toString()));
      }

      if (!accessToken.isEmpty()) {
        builder.setAccessToken(new AccessToken(accessToken, 0L, null));
      }

      builder.setDefaultConnectTimeout(120000);
      builder.setDefaultReadTimeout(120000);
      builder.setUploadConnectTimeout(0);
      builder.setUploadConnectTimeout(0);

      return builder.build();
    }
  }

  class IngestAction {
    int events;
    int payloadSize;
    byte[] payload;
    String instanceUri;
    String accessToken;
    String streamId;
    String timeStampRecordFile;
    int numThreads;
    List<IngestThread> threadList;

    public IngestAction(int events, int payloadSize, String streamName, String instanceUri, String accessToken,
                        int concurrency, String filePath) {
      this.events = events;
      this.payloadSize = payloadSize;
      this.instanceUri = instanceUri;
      this.streamId = streamName;
      this.accessToken = accessToken;
      this.payload = new byte[payloadSize];
      this.timeStampRecordFile = filePath;
      this.numThreads = concurrency;
      this.threadList = Lists.newArrayList();

    }

    public void send() throws Exception {

      // read the existing file and for n-threads populate the last recorded timestamp map.
      Map<Integer, Long> lastProcessedTimeStamps = Maps.newHashMap();
      BufferedReader reader = Files.newReader(new File(timeStampRecordFile), Charsets.UTF_8);
      try {
        String line = reader.readLine();
        while (line != null) {
          if (line.startsWith("thread")) {
            String[] parts = line.split(",");
            int threadId = Integer.parseInt(parts[0].split(":")[1]);
            long timeStamp = Long.parseLong(parts[1].split(":")[1]);
            lastProcessedTimeStamps.put(threadId, timeStamp);
          }
          line = reader.readLine();
        }
      } finally {
        reader.close();
      }

      // todo : add param to skip this check
      if (lastProcessedTimeStamps.size() != 0 && lastProcessedTimeStamps.size() != numThreads) {
        throw new Exception("Number of threads before termination is different, " +
                              "either configure it to be same or delete the contents " +
                              "of the file if catchup is not needed");
      }

      System.out.println("timestamp map is " + lastProcessedTimeStamps);
      for (int t = 1; t <= numThreads; t++) {
        long timeStamp = lastProcessedTimeStamps.get(t) == null ?
          System.currentTimeMillis() : lastProcessedTimeStamps.get(t);
        threadList.add(new IngestThread(events, payloadSize, streamId, instanceUri,
                                        accessToken, timeStampRecordFile, t, timeStamp));
      }

      // start all the threads
      for (IngestThread thread : threadList) {
        thread.start();
      }
    }

    public void stop() throws IOException, InterruptedException {
      if (threadList.size() > 0) {
        java.io.File inputFile = new java.io.File(timeStampRecordFile);
        BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile));
        try {
          for (IngestThread thread : threadList) {
            final CountDownLatch waitLatch = new CountDownLatch(1);
            thread.stopThread(writer, waitLatch);
            waitLatch.await();
          }
        } finally {
          writer.close();
        }
      }
    }
  }

}
