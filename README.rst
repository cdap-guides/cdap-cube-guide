============================
Data Analysis with OLAP Cube
============================

The `Cask Data Application Platform (CDAP) <http://cdap.io>`__ provides a number of
pre-packaged Datasets, which make it easy to store and retrieve data using
best-practices-based implementations of common data access patterns. In this guide, you
will learn how to store multidimensional data points in a Cube dataset
(see `OLAP Cube <http://en.wikipedia.org/wiki/OLAP_cube>`__), and then perform
queries with it. For analysis, we’ll be using an example of processing web logs.

An OLAP (Online Analytical Processing) Cube is multidimensional database or array,
optimized for data warehousing and OLAP applications.

What You Will Build
===================

This guide will take you through building a simple `CDAP application
<http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/applications.html>`__ 
that ingests web logs, aggregates the request counts for different combinations of fields,
and that can then be queried for the volume over a time period. You can then retrieve
insights on the traffic of a web site and the web site’s health. You will:

- Use a
  `Stream <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/streams.html>`__
  to ingest real-time log data;
- Build a
  `Flow <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/flows-flowlets/flows.html>`__
  to process log entries as they are received into multidimensional facts;
- Use a
  `Dataset <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/datasets/index.html>`__
  to store the aggregated numbers; and
- Build a
  `Service <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/services.html>`__
  to query the aggregated data across multiple dimensions.

What You Will Need
==================

- `JDK 7 or 8 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__
- `Apache Maven 3.1+ <http://maven.apache.org/>`__
- `CDAP SDK <http://docs.cdap.io/cdap/current/en/developers-manual/getting-started/standalone/index.html>`__

Let’s Build It!
===============

The following sections will guide you through building an application from scratch. If you
are interested in deploying and running the application right away, you can clone its
source code from this GitHub repository. In that case, feel free to skip the next two
sections and jump right to the `Build and Run Application <#build-and-run-application>`__
section.

Application Design
------------------
For this guide we will assume we are processing logs of a web-site that are produced by an
Apache web server. The data could be collected from multiple servers and then sent to our
application over HTTP. There are a number of `tools
<http://docs.cdap.io/cdap/current/en/developers-manual/ingesting-tools/index.html>`__ that
can help you with the ingestion task. We’ll skip over the details of ingesting the data
(as this is `covered elsewhere
<http://docs.cask.io/cdap/current/en/examples-manual/index.html>`__) and instead focus on
storing and retrieving the data.

The application consists of these components:

.. image:: docs/images/app-design.png
   :width: 8in
   :align: center

Weblogs are sent to a ``weblogs`` Stream that is consumed by a ``CubeWriterFlow``. 
The ``CubeWriterFlow`` has a single ``CubeWriterFlowlet`` that parses a ``StreamEvent``\’s 
body into a ``CubeFact`` and writes it into a ``weblogsCube`` Dataset. The Dataset 
is configured to aggregate data for specific combinations of dimensions of a
``CubeFact`` and provides a querying interface over the stored aggregations. 
The application uses a ``CubeService`` to provide an HTTP interface for querying 
the ``weblogsCube``.

Implementation
--------------
The first step is to set up our application structure. We will use a standard 
Maven project structure for all of the source code files::

    ./pom.xml
    ./src/main/java/co/cask/cdap/guides/cube/CubeHandler.java
    ./src/main/java/co/cask/cdap/guides/cube/CubeWriterFlow.java
    ./src/main/java/co/cask/cdap/guides/cube/CubeWriterFlowlet.java
    ./src/main/java/co/cask/cdap/guides/cube/WebAnalyticsApp.java

The application is identified by the ``WebAnalyticsApp`` class. This class extends 
`AbstractApplication <http://docs.cdap.io/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html>`__,
and overrides the ``configure()`` method to define all of the application components:

.. code:: java

  public class WebAnalyticsApp extends AbstractApplication {
    static final String APP_NAME = "WebAnalyticsApp";
    static final String STREAM_NAME = "weblogs";
    static final String CUBE_NAME = "weblogsCube";
    static final String SERVICE_NAME = "CubeService";
    
    @Override
    public void configure() {
      setName(APP_NAME);
  
      addStream(new Stream(STREAM_NAME));

      // configure the Cube dataset
      DatasetProperties props = DatasetProperties.builder()
        .add("dataset.cube.resolutions", "1,60,3600")
        .add("dataset.cube.aggregation.agg1.dimensions", "response_status")
        .add("dataset.cube.aggregation.agg2.dimensions", "ip,browser")
        .build();
      createDataset(CUBE_NAME, Cube.class, props);

      addFlow(new CubeWriterFlow());
      addService(SERVICE_NAME, new CubeHandler());
    }
  }

First, we need a place to receive and process the events. CDAP provides a 
`real-time stream processing system <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/flows-flowlets/index.html>`__
that is a great match for handling event streams. After first setting 
the application name, our ``WebAnalyticsApp`` adds a new 
`Stream <http://docs.cdap.io/cdap/current/en/developers-manual/building-blocks/streams.html>`__.

Then, the application configures a Cube dataset to compute and store 
aggregations for combinations of dimensions. Let’s take a closer
look at the properties that are used to configure the Cube dataset:

.. code:: java

    .add("dataset.cube.resolutions", "1,60,3600")
    .add("dataset.cube.aggregation.agg1.dimensions", "response_status")
    .add("dataset.cube.aggregation.agg2.dimensions", "ip,browser")

A Cube dataset can compute aggregates for multiple time resolutions to provide 
a better view of data for both small and large time ranges. For example, you may want to see 
data points for each second for the last five minutes, while to build a sensible 
chart for a report that covers a week, you may need to see per-hour aggregations. 

The code above defines three resolutions: 1 second, 1 minute (60 seconds), 
and 1 hour (3600 seconds). When querying the Cube data, you can specify any of 
these three depending on your need.

Each aggregation in a Cube is defined by a list of dimensions, which can later be used
for querying. The above code defines two aggregations: “agg1” and agg2”. The first 
has only one dimension: *response_status*. Thus, the Cube will allow queries such as
“number of requests that had a response status 200” or “number of requests for 
each response status”.

The second aggregation (“agg2”) defines two dimensions: *ip* and *browser*, which allows
querying by ip, by browser, or by using both together, as we shall see below.

After the Cube dataset is configured, the application adds a ``CubeWriterFlow`` to compute
``CubeFact``\ s from the ``StreamEvent``\ s and write them to the Cube, and a
``CubeService`` that has a single handler that provides an HTTP API to query the Cube. 

Let’s take a closer look at these two.

CubeWriterFlow
..............

.. code:: java

  public class CubeWriterFlow implements Flow {
    static final String FLOW_NAME = "CubeWriterFlow";

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName(FLOW_NAME)
        .setDescription("Reads logs from a Stream and writes them to a Cube dataset")
        .withFlowlets()
          .add("writer", new CubeWriterFlowlet())
        .connect()
          .fromStream(WebAnalyticsApp.STREAM_NAME).to("writer")
        .build();
    }
  }

The Flow configures a single ``CubeWriterFlowlet`` to consume data from a Stream:

.. code:: java

  public class CubeWriterFlowlet extends AbstractFlowlet {
    private static final Pattern CLF_PATTERN =
      Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] " +
                        "\"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

    private Metrics metrics;

    @UseDataSet(WebAnalyticsApp.CUBE_NAME)
    private Cube cube;

    @ProcessInput
    public void process(StreamEvent event) throws Exception {

      String logEntryLine = Bytes.toString(event.getBody());

      Pattern p = CLF_PATTERN;
      Matcher matcher = p.matcher(logEntryLine);
      if (!matcher.matches()) {
        metrics.count("parse.errors", 1);
        return;
      }

      // creating CubeFact with timestamp of the log record
      long ts = DATE_FORMAT.parse(matcher.group(4)).getTime();
      CubeFact fact = new CubeFact(ts / 1000);

      // adding dimensions
      fact.addDimensionValue("ip", matcher.group(1));
      fact.addDimensionValue("request", matcher.group(5));
      fact.addDimensionValue("response_status", matcher.group(6));
      if (!matcher.group(8).equals("-")) {
        fact.addDimensionValue("referrer", matcher.group(8));
      }
      fact.addDimensionValue("browser", matcher.group(9));

      // adding measurements
      fact.addMeasurement("count", MeasureType.COUNTER, 1);
      Integer bytesSent = Integer.valueOf(matcher.group(7));
      fact.addMeasurement("bytes.sent", MeasureType.COUNTER, bytesSent);
      cube.add(fact);
    }
  }

The ``CubeWriterFlowlet`` uses a Cube dataset that is injected via the ``@UseDataSet``
annotation with the specified dataset name. It reports on parsing errors by utilizing a
Metrics field injected by the CDAP framework.

The Flowlet process method parses the body of the ``StreamEvent`` that contains a log
entry in a combined log format. Then, it constructs a CubeFact by adding dimensions using the
parsed field values. It adds two measurements to be computed by the Cube in every
aggregation: the “count” for the number of requests, and the “bytes.sent” for the amount
of data sent.

CubeService
...........

The ``CubeService`` added to the Application is constructed using a single handler, 
``CubeHandler``:

.. code:: java

  public final class CubeHandler extends AbstractCubeHttpHandler {
    @UseDataSet(WebAnalyticsApp.CUBE_NAME)
    private Cube cube;

    @Override
    protected Cube getCube() {
      return cube;
    }
  }


The ``AbstractCubeHttpHandler`` that is provided out-of-the-box with CDAP handles basic 
Cube methods, such as *add*, *searchDimensionValue*, *searchMeasure*, and *query*, while the subclass
only needs to return the Cube dataset itself. Below, we will see how to use the HTTP 
interface of the Service.


Build and Run Application
=========================

The ``WebAnalyticsApp`` application can be built and packaged using the Apache Maven command::

  $ mvn clean package

Note that the remaining commands assume that the ``cdap-cli.sh`` script is
available on your PATH. If that is not the case, please add it::

  $ export PATH=$PATH:<CDAP home>/bin

If you haven't already started a standalone CDAP installation, start it with the command::

  $ cdap.sh start

We can then deploy the application to a standalone CDAP installation and start ``CubeWriterFlow``
and ``CubeService``::

  $ cdap-cli.sh load artifact target/cdap-cube-guide-<version>.jar
  $ cdap-cli.sh create app WebAnalyticsApp cdap-cube-guide <version> user
  $ cdap-cli.sh start flow WebAnalyticsApp.CubeWriterFlow
  $ cdap-cli.sh start service WebAnalyticsApp.CubeService

Next, we will send some sample weblogs into the Stream for processing::
  
  $ cdap-cli.sh load stream weblogs resources/accesslog.txt

As data is being processed, we can start querying it via a RESTful API
provided by the ``CubeService``. For convenience, we’ve put the queries themselves
into separate JSON files.

Explore and Query Cube
----------------------

Many times, users may not know what data a Cube contains and require some 
exploration first to construct the queries themselves. Let’s start by searching 
for the dimension values that are available in the Cube with this ``CubeExploreQuery``:

.. code:: json

  {
      "startTs": 1423370200,
      "endTs":   1423398198,
      "resolution": 3600,
      "dimensionValues": [],
      "limit": 1000
  }

Submit::

  $ curl -w'\n' -X POST -d @resources/search-first.json 'http://localhost:10000/v3/namespaces/default/apps/WebAnalyticsApp/services/CubeService/methods/searchDimensionValue'

The result will be the dimension values of the first dimensions defined in all aggregations (reformatted
for readability):

.. code:: json

  [
      {
          "name": "ip",
          "value": "69.181.160.120"
      },
      {
          "name": "ip",
          "value": "109.63.206.34"
      },
      {
          "name": "ip",
          "value": "113.72.144.115"
      },
      {
          "name": "response_status",
          "value": "200"
      },
      {
          "name": "response_status",
          "value": "404"
      }
  ]

To drill down further into the dimension hierarchy of aggregations, let’s refine the query with a specific dimension value:

.. code:: json

  {
      "startTs": 1423370200,
      "endTs":   1423398198,
      "resolution": 3600,
      "dimensionValues": [{"name": "ip", "value": "69.181.160.120"}],
      "limit": 1000
  }

Submit::

  $ curl -w'\n' -X POST -d @resources/search-ip.json 'http://localhost:10000/v3/namespaces/default/apps/WebAnalyticsApp/services/CubeService/methods/searchDimensionValue'

The result is the dimension values of the next dimension defined in Cube aggregations:

.. code:: json

  [
      {
          "name": "browser",
          "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36"
      }
  ]

The Cube search API allows you to query for available measures via the ``searchMeasure`` endpoint::

  $ curl -w'\n' -X POST -d @resources/search-ip.json 'http://localhost:10000/v3/namespaces/default/apps/WebAnalyticsApp/services/CubeService/methods/searchMeasure'

The result contains all the measurement names:

.. code:: json

  [
      "bytes.sent",
      "count"
  ]

Now, let’s perform some data queries. Here’s how we can get the timeseries for the
number of bytes sent for a specific source ip, per each browser type:

.. code:: json

  {
      "aggregation": "agg2",
      "resolution": 3600,
      "startTs": 1423370200,
      "endTs":   1423398198,
      "measurements": {"bytes.sent": "SUM"},
      "dimensionValues": {"ip": "69.181.160.120"},
      "groupByDimensions": ["browser"],
      "limit": 1000
  }

One way of reading the query definition is this analogous SQL command:

.. code:: sql

   SELECT    sum('bytes.sent')                 -- measure name and aggregation function
   FROM      agg2.1h_res                       -- aggregation & resolution
   GROUP BY  browser                           -- groupByDimensions
   WHERE     ip='69.181.160.120' AND           -- dimensionValues
             ts>=1423370200 AND ts<1423398198  -- startTs & endTs
   LIMIT     1000                              -- limit

Submit::

  $ curl -w'\n' -X POST -d @resources/query-ip-browser.json 'http://localhost:10000/v3/namespaces/default/apps/WebAnalyticsApp/services/CubeService/methods/query'

The result is a timeseries with one data point (if any are available) per hour:

.. code:: json

  [
      {
          "measureName": "bytes.sent",
          "dimensionValues": {
              "browser": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36"
          },
          "timeValues": [
              {
                  "timestamp": 1423371600,
                  "value": 122240
              },
              {
                  "timestamp": 1423375200,
                  "value": 122240
              },
              {
                  "timestamp": 1423378800,
                  "value": 121732
              },
              {
                  "timestamp": 1423382400,
                  "value": 122240
              },
              {
                  "timestamp": 1423386000,
                  "value": 121732
              },
              {
                  "timestamp": 1423389600,
                  "value": 122240
              },
              {
                  "timestamp": 1423393200,
                  "value": 121732
              },
              {
                  "timestamp": 1423396800,
                  "value": 47327
              }
          ]
      }
  ]

The query below will help to analyse the number of errors (or invalid requests) that the web site handles:

.. code:: json

  {
      "aggregation": "agg1",
      "startTs": 1423370200,
      "endTs":   1423398198,
      "measurements": {"count": "SUM"},
      "resolution": 3600,
      "dimensionValues": {},
      "groupByDimensions": ["response_status"],
      "limit": 1000
  }

Submit::

  $ curl -w'\n' -X POST -d @resources/query-response-status.json 'http://localhost:10000/v3/namespaces/default/apps/WebAnalyticsApp/services/CubeService/methods/query'

The result is a multiple timeseries for each response status:

.. code:: json

  [
      {
          "measureName": "count",
          "dimensionValues": {
              "response_status": "200"
          },
          "timeValues": [
              {
                  "timestamp": 1423371600,
                  "value": 969
              },
              {
                  "timestamp": 1423375200,
                  "value": 360
              },
              {
                  "timestamp": 1423378800,
                  "value": 409
              },
              {
                  "timestamp": 1423382400,
                  "value": 468
              },
              {
                  "timestamp": 1423386000,
                  "value": 465
              },
              {
                  "timestamp": 1423389600,
                  "value": 468
              },
              {
                  "timestamp": 1423393200,
                  "value": 471
              },
              {
                  "timestamp": 1423396800,
                  "value": 186
              }
          ]
      },
      {
          "measureName": "count",
          "dimensionValues": {
              "response_status": "404"
          },
          "timeValues": [
              {
                  "timestamp": 1423375200,
                  "value": 2
              },
              {
                  "timestamp": 1423378800,
                  "value": 2
              },
              {
                  "timestamp": 1423386000,
                  "value": 2
              },
              {
                  "timestamp": 1423393200,
                  "value": 2
              }
          ]
      }
  ]

We can see there are just a few "404" responses, which is likely normal for such a
well-managed website(!).

Changing the Cube Configuration
-------------------------------

As applications evolve, we may need to change the Cube aggregation configuration to either
support new queries or to optimize existing ones. Let’s see how you can add an 
aggregation to an existing Cube.

We’d like the configuration changed to include these properties:

.. code:: json

  {
      "typeName":"co.cask.cdap.api.dataset.lib.cube.Cube",
      "properties": {
          "dataset.cube.resolutions":"1,60,3600",
          "dataset.cube.aggregation.agg1.dimensions":"response_status",
          "dataset.cube.aggregation.agg2.dimensions":"ip,browser",
          "dataset.cube.aggregation.agg3.dimensions":"referrer",
          "dataset.cube.aggregation.agg3.requiredDimensions":"referrer"
      }
  }

We’ve added *agg3* that computes statistics for referrers. Note the extra property that ends 
with *requiredDimensions*: it specifies to only use this aggregation if the required dimension is present in a CubeFact.
You may have noticed that in ``CubeWriterFlowlet``, the referrer field may be empty in a log entry. 
We don’t want to store extra aggregates in the fact where this is the case.

Let’s update the dataset configuration, and then restart both the Flow and the Service so that the change takes effect::

  $ curl -w'\n' -X PUT -d @resources/cube-config.json 'http://localhost:10000/v3/namespaces/default/data/datasets/weblogsCube/properties'
  $ cdap-cli.sh stop flow WebAnalyticsApp.CubeWriterFlow
  $ cdap-cli.sh start flow WebAnalyticsApp.CubeWriterFlow
  $ cdap-cli.sh stop service WebAnalyticsApp.CubeService
  $ cdap-cli.sh start service WebAnalyticsApp.CubeService

Let’s send additional data to compute new aggregations::

  $ cdap-cli.sh load stream weblogs resources/accesslog.txt

Now, we can retrieve statistics on referrers using the newly-added aggregation:

.. code:: json

  {
      "aggregation": "agg3",
      "startTs": 1423370200,
      "endTs":   1423398198,
      "measurements": {"count": "SUM"},
      "resolution": 3600,
      "dimensionValues": {"referrer": "http://cdap.io/"},
      "groupByDimensions": [],
      "limit": 1000
  }

Submit::
  
  $ curl -w'\n' -X POST -d @resources/query-referrer.json 'http://localhost:10000/v3/namespaces/default/apps/WebAnalyticsApp/services/CubeService/methods/query'

Result:

.. code:: json

  [
      {
          "measureName": "count",
          "dimensionValues": {},
          "timeValues": [
              {
                  "timestamp": 1423375200,
                  "value": 3
              },
              {
                  "timestamp": 1423389600,
                  "value": 1
              }
          ]
      }
  ]


Share and Discuss!
==================

Have a question? Discuss at the `CDAP User Mailing List <https://groups.google.com/forum/#!forum/cdap-user>`__.

License
=======

Copyright © 2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
