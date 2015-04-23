=======================
Storing Timeseries Data
=======================

The `Cask Data Application Platform (CDAP) <http://cdap.io>`__ provides a
number of pre-packaged Datasets, which make it easy to store and
retrieve data using best-practices-based implementations of common data
access patterns. In this guide, you will learn how to process and store
timeseries data, using the example of real-time sensor data from a
traffic monitor network.

What You Will Build
===================

This guide will take you through building a simple
`CDAP application <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/applications.html>`__
to ingest data from a sensor network of traffic monitors, aggregate the
event counts into a traffic volume per road segment, and query the
traffic volume over a time period to produce a traffic condition report.
You will:

- Use a
  `Stream <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/streams.html>`__
  to ingest real-time events data;
- Build a
  `Flow <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/flows-flowlets/flows.html>`__
  to process events as they are received, and count by road segment and
  event type;
- Use a
  `Dataset <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/datasets/index.html>`__
  to store the event data; and
- Build a
  `Service <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/services.html>`__
  to retrieve the event counts by time range.

What You Will Need
==================

- `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__
- `Apache Maven 3.0+ <http://maven.apache.org/>`__
- `CDAP SDK <http://docs.cdap.io/cdap/current/en/developers-manual/getting-started/standalone/index.html>`__

Let’s Build It!
===============

The following sections will guide you through building an application from scratch. If you
are interested in deploying and running the application right away, you can clone its
source code and binaries from this GitHub repository. In that case, feel free to skip the
next two sections and jump right to the
`Build and Run Application <#build-and-run-application>`__ section.

Application Design
------------------
For this guide, we will assume that we are processing events from a
sensor network of traffic monitors. Each traffic monitor covers a given
road segment and provides periodic reports of the number of passing
vehicles, and a count of any traffic accidents that have occurred.

Sensors report in from the network by sending event records containing
the following fields:

- ``road_segment_id``: ``LONG`` (unique identifier for the road segment)
- ``timestamp``: ``YYYY-MM-DD hh:mm:ss`` formatted
- ``event_type``:

    - ``VEHICLE``: indicates a count of vehicles passing the sensor since the last report
    - ``ACCIDENT``: indicates a count of traffic accidents since the last report
      
- ``count``: ``INT``

The application consists of the following components:

.. image:: docs/images/app-design.png
   :width: 8in
   :align: center

Incoming events feed into the application through a Stream. CDAP
provides a RESTful API for ingesting events into a Stream.

Once fed into the Stream, events are processed by the ``TrafficEventParser``
Flowlet, which normalizes and validates the event data, transforming the
stream entry into a ``TrafficEvent`` object. Parsed ``TrafficEvent``\ s are
then passed along to the ``TrafficEventSink`` Flowlet, which stores the
event counts in a Timeseries Dataset. The Timeseries Dataset aggregates
the event counts by road segment ID and time window.

In addition to storing the sensor data as a timeseries, we also want to
query the recent traffic data in order to provide traffic condition
alerts to drivers. The ``TrafficConditionService`` exposes an HTTP RESTful API to
support this.

Implementation
--------------
The first step is to get our application structure set up. We will use a
standard Maven project structure for all of the source code files::

    ./pom.xml
    ./src/main/java/co/cask/cdap/guides/traffic/TrafficApp.java
    ./src/main/java/co/cask/cdap/guides/traffic/TrafficConditionService.java
    ./src/main/java/co/cask/cdap/guides/traffic/TrafficEvent.java
    ./src/main/java/co/cask/cdap/guides/traffic/TrafficEventParser.java
    ./src/main/java/co/cask/cdap/guides/traffic/TrafficEventSink.java
    ./src/main/java/co/cask/cdap/guides/traffic/TrafficFlow.java

The application is identified by the ``TrafficApp`` class. This class extends
`AbstractApplication 
<http://docs.cdap.io/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html>`__,
and overrides the ``configure()`` method to define all of the application components:

.. code:: java

  public class TrafficApp extends AbstractApplication {
    public static final String APP_NAME = "TrafficApp";
    public static final String STREAM_NAME = "trafficEvents";
    public static final String TIMESERIES_TABLE_NAME = "trafficEventTable";   
    public static final int TIMESERIES_INTERVAL = 15 * 60 * 1000; // 15 minutes 

    @Override
    public void configure() {
      setName(APP_NAME);

      addStream(new Stream(STREAM_NAME));
      // configure the timeseries table
      DatasetProperties props =
        TimeseriesTables.timeseriesTableProperties(TIMESERIES_INTERVAL,
                                                   DatasetProperties.EMPTY);
      createDataset(TIMESERIES_TABLE_NAME, CounterTimeseriesTable.class, props);
      addFlow(new TrafficFlow());
      addService(new TrafficConditionService());
    }
  }

When it comes to handling time-based events, we need a place to receive
and process the events themselves. CDAP provides a `real-time stream
processing system <http://docs.cdap.io/cdap/current/en/dev-guide.html#flows>`__ that
is a great match for handling event streams. After first setting
the application name, our ``TrafficApp`` adds a new
`Stream <http://docs.cdap.io/cdap/current/en/dev-guide.html#streams>`__.

We also need a place to store the traffic event records that we receive;
``TrafficApp`` next creates a Dataset to store the processed data.
``TrafficApp`` uses a `CounterTimeseriesTable 
<http://docs.cdap.io/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/dataset/lib/CounterTimeseriesTable.html>`__,
which orders data by a key plus a timestamp. This makes it possible to
efficiently query the reported values for a given time range.

Finally, ``TrafficApp`` adds a
`Flow <http://docs.cdap.io/cdap/current/en/dev-guide.html#flows>`__ to
process data from the Stream, and a
`Service <http://docs.cdap.io/cdap/current/en/dev-guide.html#services>`__
to query the traffic events that have been processed and stored.

The incoming traffic events are processed in two phases, defined in the
``TrafficFlow`` class by building a ``FlowSpecification`` in the ``configure()``
method:

.. code:: java

  public class TrafficFlow implements Flow {
    public static final String FLOW_NAME = "TrafficFlow";

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName(FLOW_NAME)
        .setDescription("Reads traffic events from a stream and persists to a timeseries dataset")
        .withFlowlets()
          .add("parser", new TrafficEventParser())
          .add("sink", new TrafficEventSink())
        .connect()
          .fromStream(TrafficApp.STREAM_NAME).to("parser")
          .from("parser").to("sink")
        .build();
    }
  }

``TrafficFlow`` first registers the two `Flowlets 
<http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/flows-flowlets/flowlets.html>`__
to be used in the specification, then connects the registered Flowlets
into a processing pipeline. The first Flowlet, ``TrafficEventParser``, reads
raw events from the Stream, parses and validates the individual fields,
and then emits the structured event objects. The second flowlet, ``TrafficEventSink``,
receives the structured events from ``TrafficEventParser``, and stores them
to the ``CounterTimeseriesTable`` Dataset.

The ``TrafficEvent`` passed between the Flowlets is a simple POJO (getters
and setters have been omitted in this code fragment):

.. code:: java

  public class TrafficEvent {
    public enum Type { VEHICLE, ACCIDENT };

    private final String roadSegmentId;
    private final long timestamp;
    private final Type type;
    private final int count;
    ...
  }

First, let’s look at ``TrafficEventParser`` in more detail:

.. code:: java

  public class TrafficEventParser extends AbstractFlowlet {
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; 

    private final DateFormat df = new SimpleDateFormat(DATE_FORMAT);

    private Metrics metrics;
    private OutputEmitter<TrafficEvent> out;

    @ProcessInput
    public void process(StreamEvent event) {
      String body = Charsets.UTF_8.decode(event.getBody()).toString();
      String[] parts = body.split("\\s*,\\s*");
      if (parts.length != 4) {
        metrics.count("event.bad", 1);
        return;
      } 

      long timestamp;
      try {
        if ("now".equalsIgnoreCase(parts[1])) {
          timestamp = System.currentTimeMillis();
        } else {
          timestamp = df.parse(parts[1]).getTime();
        }
      } catch (ParseException pe) {
        metrics.count("event.bad", 1);
        return;
      }
      TrafficEvent.Type type;
      try {
        type = TrafficEvent.Type.valueOf(parts[2]);
      } catch (IllegalArgumentException iae) {
        metrics.count("event.bad", 1);
        return;
      }
      int count;
      try {
        count = Integer.parseInt(parts[3]);
      } catch (NumberFormatException nfe) {
        metrics.count("event.bad", 1);
        return;
      } 

      out.emit(new TrafficEvent(parts[0], timestamp, type, count));
    }
  }

The ``process()`` method is annotated with ``@ProcessInput``, telling CDAP that
this method should be invoked for incoming events. Since
``TrafficEventParser`` is connected to the Stream, it receives events of
type ``StreamEvent``. Each ``StreamEvent`` contains a request body with the raw
input data, which we expect in the format::

    <road segment ID>, <timestamp>, <type>, <count>

The ``process()`` method validates each field for the correct type,
constructs a new ``TrafficEvent`` object, and emits the object to any
downstream Flowlets using the defined `OutputEmitter 
<http://docs.cdap.io/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/flow/flowlet/OutputEmitter.html>`__
instance.

The next step in the pipeline is the ``TrafficEventSink`` Flowlet:

.. code:: java

  public class TrafficEventSink extends AbstractFlowlet {
    @UseDataSet(TrafficApp.TIMESERIES_TABLE_NAME)
    private CounterTimeseriesTable table;

    @ProcessInput
    public void process(TrafficEvent event) {
      table.increment(Bytes.toBytes(event.getRoadSegmentId()),
                      event.getCount(),
                      event.getTimestamp(),
                      Bytes.toBytes(event.getType().name()));
    }
  }

In order to access the ``CounterTimeseriesTable`` used by the application,
``TrafficEventSink`` declares a variable with the `\@UseDataSet 
<http://docs.cdap.io/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/annotation/UseDataSet.html>`__
annotation and the name used to create the Dataset in ``TrafficApp``. This
variable will be injected with a reference to the ``CounterTimeseriesTable``
instance when the Flowlet runs.

``TrafficEventSink`` also defines a ``process()`` method, annotated with `\@ProcessInput 
<http://docs.cdap.io/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/annotation/ProcessInput.html>`__,
for handling incoming events from ``TrafficEventParser``. Since
``TrafficEventParser`` emits ``TrafficEvent`` objects, the process method
takes an input parameter of the same type. Here, we simply increment a
counter for the incoming event, using the road segment ID as the key,
and adding the event type (VEHICLE or ACCIDENT) as a tag. When querying
records out of the ``CounterTimeseriesTable``, we can specify the required
tags as an additional filter on the records to return. Only those
entries having all of the given tags will be returned in the results.

Now that we have the full pipeline setup for ingesting data from our
traffic sensors, we are ready to create a Service to query the traffic
sensor reports in response to real-time requests. This Service will take
a given road segment ID as input, query the road segment's recent data,
and respond with a simple classification of how congested that segment
currently is, according to these rules:

-   If any traffic accidents were reported, return RED;
-   If two or more vehicle count reports are greater than the threshold, return RED;
-   If one vehicle count report is greater than the threshold, return YELLOW;
-   Otherwise, return GREEN.

``TrafficConditionService`` defines a simple HTTP RESTful endpoint to perform
this query and return a response:

.. code:: java

  public class TrafficConditionService extends AbstractService {
    public enum Condition {GREEN, YELLOW, RED};

    public static final String SERVICE_NAME = "TrafficConditions";

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      useDataset(TrafficApp.TIMESERIES_TABLE_NAME);
      addHandler(new TrafficConditionHandler());
    }

    @Path("/v1")
    public static final class TrafficConditionHandler extends 
        AbstractHttpServiceHandler {
      private static final int CONGESTED_THRESHOLD = 100;
      private static final long LOOKBACK_PERIOD =
          TrafficApp.TIMESERIES_INTERVAL * 3;

      @UseDataSet(TrafficApp.TIMESERIES_TABLE_NAME)
      private CounterTimeseriesTable table;

      @Path("road/{segment}/recent")
      @GET
      public void recentConditions(HttpServiceRequest request, 
                                   HttpServiceResponder responder,
                                   @PathParam("segment") String segmentId) {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - LOOKBACK_PERIOD;

        Condition currentCondition = Condition.GREEN;
        int accidentEntries =
          getCountsExceeding(segmentId, startTime, endTime, 
                             TrafficEvent.Type.ACCIDENT, 0);
        if (accidentEntries > 0) {
          currentCondition = Condition.RED;
        } else {
          int congestedEntries =
            getCountsExceeding(segmentId, startTime, endTime,
                               TrafficEvent.Type.VEHICLE, CONGESTED_THRESHOLD);
          if (congestedEntries > 1) {
            currentCondition = Condition.RED;
          } else if (congestedEntries > 0) {
            currentCondition = Condition.YELLOW;
          }
        }
        responder.sendString(currentCondition.name());
      }

      private int getCountsExceeding(String roadSegmentId,
                                     long startTime, long endTime,
                                     TrafficEvent.Type type, long threshold) {
        int count = 0;
        Iterator<CounterTimeseriesTable.Counter> events =
          table.read(Bytes.toBytes(roadSegmentId), startTime, endTime, 
                     Bytes.toBytes(type.name()));
        while (events.hasNext()) {
          if (events.next().getValue() > threshold) {
            count++;
          }
        }
        return count;
      }
    }
  }

In the ``configure()`` method, ``TrafficConditionService`` defines a handler
class, ``TrafficConditionHandler``, and a Dataset to use in serving requests.
``TrafficConditionHandler`` once again makes use of the ``@UseDataSet``
annotation on an instance variable to obtain a reference to the
``CounterTimeseriesTable`` Dataset where traffic events are persisted.

The core of the service is the ``recentConditions()`` method.
``TrafficConditionHandler`` exposes this method as a RESTful endpoint through the
use of JAX-RS annotations. The ``@Path`` annotation defines the URL to which
the endpoint will be mapped, while the ``@GET`` annotation defines the HTTP
request method supported. The ``recentConditions()`` method declares
``HttpServiceRequest`` and ``HttpServiceResponder`` parameters to,
respectively, provide access to request elements and to control the
response output. The ``@PathParam`` ("segment") annotation on the third
method parameter provides access to the ``{segment}`` path element as an
input parameter.

The ``recentConditions()`` method first queries the timeseries Dataset for
any accident reports for the given road segment in the past 45 minutes.
If any are found, a "RED" condition report will be returned. If no
accident reports are present, it continues to query the timeseries
data for the number of vehicle report entries that exceed a set
threshold (100). Based on the number of entries found, the method
returns the appropriate congestion level according to the rules
previously described.

Build and Run Application
=========================

The ``TrafficApp`` application can be built and packaged using the Apache Maven command::

  $ mvn clean package

Note that the remaining commands assume that the ``cdap-cli.sh`` script is
available on your PATH. If this is not the case, please add it::

  $ export PATH=$PATH:<CDAP home>/bin

If you haven't already started a standalone CDAP installation, start it with the command::

  $ cdap.sh start

We can then deploy the application to a standalone CDAP installation::

  $ cdap-cli.sh deploy app target/cdap-timeseries-guide-<version>.jar
  $ cdap-cli.sh start flow TrafficApp.TrafficFlow

Next, we will send some sample records into the stream for processing::

  $ cdap-cli.sh send stream trafficEvents \"1N1, now, VEHICLE, 10\"
  $ cdap-cli.sh send stream trafficEvents \"1N2, now, VEHICLE, 101\"
  $ cdap-cli.sh send stream trafficEvents \"1N3, now, ACCIDENT, 1\"

We can now start the TrafficConditions service and check the service
calls::

  $ cdap-cli.sh start service TrafficApp.TrafficConditions

Since the service methods are exposed as a RESTful API, we can check the
results using the curl command::

  $ export SERVICE_URL=http://localhost:10000/v3/namespaces/default/apps/TrafficApp/services/TrafficConditions/methods
  $ curl -w'\n' $SERVICE_URL/v1/road/1N1/recent
  $ curl -w'\n' $SERVICE_URL/v1/road/1N2/recent
  $ curl -w'\n' $SERVICE_URL/v1/road/1N3/recent
  
Example output::

    GREEN
    YELLOW
    RED

or, using the CDAP CLI:

  $ cdap-cli.sh call service TrafficApp.TrafficConditions GET 'v1/road/1N1/recent'
  $ cdap-cli.sh call service TrafficApp.TrafficConditions GET 'v1/road/1N2/recent'
  $ cdap-cli.sh call service TrafficApp.TrafficConditions GET 'v1/road/1N3/recent'

+======================================================================================+
| status        | headers                            | body size      | body           |
+======================================================================================+
| 200           | Content-Length : 5                 | 5              | GREEN          |
|               | Connection : keep-alive            |                |                |
|               | Content-Type : text/plain; charset |                |                |
|               | =UTF-8                             |                |                |
+======================================================================================+

Congratulations! You have now learned how to incorporate timeseries data
into your CDAP applications. Please continue to experiment and extend
this sample application. The ability to store and query time-based data
can be a powerful tool in many scenarios.

Extend This Example
===================

- Write a MapReduce job to look at traffic volume over the last 30 days
  and store the average traffic volume for each 15 minute time slot in the
  day into another data set. 
- Modify the ``TrafficService`` to look at the average traffic volumes and use these to
  identify when traffic is congested.

Share and Discuss!
==================

Have a question? Discuss at the `CDAP User Mailing List <https://groups.google.com/forum/#!forum/cdap-user>`__.

License
=======

Copyright © 2014-2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
