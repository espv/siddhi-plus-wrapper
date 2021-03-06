# Siddhi Extensions

## Available Extensions

Following are some pre-written extensions that are supported with Siddhi;

### Extensions released under Apache 2.0 License
Name | Description
:-- | :--
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-approximate">execution-approximate</a> | The siddhi-execution-approximate is an extension to Siddhi that performs approximate computing on event streams.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env">execution-env</a> | The siddhi-execution-env extension is an extension to Siddhi that provides the capability to read environment properties inside Siddhi stream definitions and use it inside queries. Functions of the env extension are as follows..
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema">execution-extrema</a> | The siddhi-execution-extrema extension is an extension to Siddhi that processes event streams based on different arithmetic properties. Different types of processors are available to extract the extremas from the event streams according to the specified attribute in the stream.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-geo">execution-geo</a> | The siddhi-execution-geo extension is an extension to Siddhi that provides geo data related functionality such as checking whether a given geo coordinate is within a predefined geo-fence, etc. Following are the functions of the Geo extension.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-graph">execution-graph</a> | The siddhi-execution-graph extension is an extension to Siddhi that provides graph related functionality to Siddhi such as getting size of largest connected component of a graph, maximum clique size of a graph, etc.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-kalmanfilter">execution-kalmanfilter</a> | The siddhi-execution-kalman-filter extension is an extension to Siddhi provides that Kalman filtering capabilities to Siddhi. This allows you to detect outliers of input data.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-map">execution-map</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-markov">execution-markov</a> | The siddhi-execution-markov extension is an extension to Siddhi that allows abnormal patterns relating to user activity to be detected when carrying out real time analysis.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-math">execution-math</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-priority">execution-priority</a> | The siddhi-execution-priority extension is an extension to Siddhi that keeps track of the priority of events in a stream.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-regex">execution-regex</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-reorder">execution-reorder</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-sentiment">execution-sentiment</a> | The siddhi-execution-sentiment extension is an extension to Siddhi that performs sentiment analysis using Afinn Wordlist-based approach.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-stats">execution-stats</a> | The siddhi-execution-stats extension is an extension to Siddhi that provides statistical functions for aggregated events. Currently this contains median function which calculate the median of aggregated events. Calculation of median is done for each event arrival and expiry, it is not recommended to use this extension for large window sizes.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-streamingml">execution-streamingml</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-string">execution-string</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-tensorflow">execution-tensorflow</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-time">execution-time</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries">execution-timeseries</a> | The siddhi-execution-timeseries extension is an extension to Siddhi which enables users to forecast and detect outliers in time series data, using Linear Regression Models.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique">execution-unique</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unitconversion">execution-unitconversion</a> | The siddhi-execution-unitconversion extension is an extension to Siddhi that enables conversions of length, mass, time and volume units.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-cdc">io-cdc</a> | The siddhi-io-cdc extension is an extension to Siddhi. It receives change data from MySQL, MS SQL Server, Postgresql, H2 and Oracle in the key-value format.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email">io-email</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-file">io-file</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-http">io-http</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-jms">io-jms</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka">io-kafka</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-mqtt">io-mqtt</a> | The siddhi-io-mqtt is an extension to Siddhi mqtt source and sink implementation,that publish and receive events from mqtt broker.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-prometheus">io-prometheus</a> | The siddhi-io-prometheus extension is an extension to Siddhi. The Prometheus-sink publishes Siddhi events as Prometheus metrics and expose them to Prometheus server. The Prometheus-source retrieves Prometheus metrics from an endpoint and send them as Siddhi events.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-rabbitmq">io-rabbitmq</a> | The siddhi-io-rabbitmq is an extension to Siddhi that publish and receive events from rabbitmq broker.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-sqs">io-sqs</a> | The siddhi-io-sqs extension is an extension to Siddhi that used to receive and publish events via AWS SQS Service. This extension allows users to subscribe to a SQS queue and receive/publish SQS messages.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-tcp">io-tcp</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-twitter">io-twitter</a> | The siddhi-io-twitter extension is an extension to Siddhi. It publishes event data from Twitter Applications in the key-value map format.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-websocket">io-websocket</a> | The siddhi-io-websocket extension is an extension to Siddhi that allows to receive and publish events through WebSocket.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-wso2event">io-wso2event</a> | The siddhi-io-wso2event extension is an extension to Siddhi that receives and publishes events in the WSO2Event format via Thrift or Binary protocols.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-binary">map-binary</a> | The siddhi-map-binary extension is an extension to Siddhi that can be used to convert binary events to/from Siddhi events.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-csv">map-csv</a> | The siddhi-map-csv extension is an extension to Siddhi that supports mapping incoming events with csv format to a stream at the source, and mapping a stream to csv format events at the sink.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-json">map-json</a> | The siddhi-map-json extension is an extension to Siddhi which is used to convert JSON message to/from Siddhi events.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-keyvalue">map-keyvalue</a> | The siddhi-map-keyvalue extension is an extension to Siddhi that supports mapping incoming events with Key-Value map format to a stream at the source, and mapping a stream to Key-Value map events at the sink.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-text">map-text</a> | The siddhi-map-text extension is an extension to Siddhi that supports mapping incoming text messages to a stream at the source, and mapping a stream to text messages at the sink.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-wso2event">map-wso2event</a> | The siddhi-map-wso2event extension is an extension to Siddhi that can be used to convert WSO2 events to/from Siddhi events.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-xml">map-xml</a> | The siddhi-map-xml extension is an extension to Siddhi that supports mapping incoming XML events to a stream at the source, and mapping a stream to XML events at the sink.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-script-js">script-js</a> | The siddhi-script-js is an extension to Siddhi that allows to include JavaScript functions within the Siddhi Query Language.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-cassandra">store-cassandra</a> | The siddhi-store-cassandra extension is an extension to Siddhi that can be used to persist events to a Cassandra instance of the users choice. Find some useful links below:
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-hbase">store-hbase</a> | The siddhi-store-hbase extension is an extension to Siddhi that can be used to persist events to a HBase instance of the users choice. Find some useful links below:
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-mongodb">store-mongodb</a> | The siddhi-store-mongodb extension is an extension to Siddhi that can be used to persist events to a MongoDB instance of the users choice. Find some useful links below:
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-rdbms">store-rdbms</a> | The siddhi-store-rdbms extension is an extension to Siddhi that can be used to persist events to an RDBMS instance of the user's choice.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-redis">store-redis</a> | The siddhi-store-redis extension is an extension for siddhi redis event table implementation. This extension can be used to persist events to a redis cloud instance of version 4.x.x.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-solr">store-solr</a> | The siddhi-store-solr extension is an extension for siddhi Solr event table implementation. This extension can be used to persist events to a Solr cloud instance of version 6.x.x.

### Extensions released under GPL License
Name | Description
:-- | :--
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-geo">execution-geo</a> | The siddhi-gpl-execution-geo extension is an extension to Siddhi that provides geo data related functionality such as checking whether a given geo coordinate is within a predefined geo-fence, etc.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-nlp">execution-nlp</a> | The siddhi-gpl-execution-nlp extension is an extension to Siddhi that can be used to process natural language.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-pmml">execution-pmml</a> | 
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-r">execution-r</a> | The siddhi-gpl-execution-r extension is an extension to Siddhi that can be used to process events with R scripts.
<a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-streamingml">execution-streamingml</a> | The siddhi-execution-streamingml is an extension to Siddhi that performs streaming machine learning on event streams.


## Extension Repositories

All the extension repositories maintained by WSO2 can be found <a target="_blank" href="https://github.com/wso2-extensions/?utf8=%E2%9C%93&q=siddhi&type=&language=">here</a>
