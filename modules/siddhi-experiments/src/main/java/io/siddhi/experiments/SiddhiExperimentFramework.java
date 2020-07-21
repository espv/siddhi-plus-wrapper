package io.siddhi.experiments;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.experiments.utils.Tuple2;
import io.siddhi.experiments.utils.Tuple3;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import no.uio.ifi.ExperimentAPI;
import no.uio.ifi.SpeComm;
import no.uio.ifi.SpeSpecificAPI;
import no.uio.ifi.TracingFramework;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class SiddhiExperimentFramework implements ExperimentAPI, SpeSpecificAPI {
    private volatile int totalEventCount = 0;
    private volatile int threadCnt = 0;
    private int batch_size;
    private int interval_wait;
    private int pktsPublished;
    private TracingFramework tf = new TracingFramework();
    int number_threads = 1;
    long timeLastRecvdTuple = 0;
    Map<Integer, SiddhiAppRuntime> queryIdToSiddhiAppRuntime = new HashMap<>();
    SiddhiAppRuntime siddhiAppRuntime;
    Map<Integer, Boolean> streamIdActive = new HashMap<>();
    Map<Integer, Boolean> streamIdBuffer = new HashMap<>();
    SpeComm speComm;

    TCPNettyServer tcpNettyServer;
    List<StreamListener> streamListeners = new ArrayList<>();
    Map<Integer, TCPNettyClient> nodeIdToClient = new HashMap<>();
    Map<String, Integer> streamNameToId = new HashMap<>();
    Map<Integer, Map<String, Object>> nodeIdToIpAndPort = new HashMap<>();
    Map<Integer, List<Map<String, Object>>> datasetIdToTuples = new HashMap<>();

    ArrayList<Tuple3<byte[], Attribute.Type[], String>> allPackets = new ArrayList<>();
    ArrayList<Tuple2<String, StreamCallback>> allCallbacks = new ArrayList<>();
    //ArrayList<Integer> eventIDs = new ArrayList<>();

    SiddhiManager siddhiManager = new SiddhiManager();
    StringBuilder queries = new StringBuilder();
    Map<Integer, Map<String, Object>> allSchemas = new HashMap<>();
    Map<Integer, String> siddhiSchemas = new HashMap<>();
    Map<Integer, List<Integer>> streamIdToNodeIds = new HashMap<>();
    Map<Integer, BufferedWriter> streamIdToCsvWriter = new HashMap<>();
    Map<Tuple2<Integer, Integer>, List<Integer>> streamIdAndQueryIdToSourceNodes = new HashMap<>();
    Map<Integer, Map<String, Object>> queryIdToMapQuery = new HashMap<>();
    Map<Integer, List<Tuple3<String, Attribute.Type[], Event>>> streamIdToBuffer = new HashMap<>();
    int port;
    int node_id;
    private String trace_output_folder;

    //@Override
    public String SetupClientTcpServer(int port) {
        this.tcpNettyServer = new TCPNettyServer();
        this.port = port;
        ServerConfig sc = new ServerConfig();
        sc.setPort(port);
        tcpNettyServer.start(sc);
        return "Success";
    }

    public void TearDownTcpServer() {
        //this.tcpNettyServer.shutdownGracefully();
    }

    public void SetNodeId(int node_id) {
        this.node_id = node_id;
    }

    public void SetTraceOutputFolder(String f) {this.trace_output_folder = f;}

    @Override
    public String StartRuntimeEnv() {
        timeLastRecvdTuple = 0;
        StartSiddhiAppRuntime();
        return "Success";
    }

    @Override
    public String StopRuntimeEnv() {
        for (SiddhiAppRuntime runtime : queryIdToSiddhiAppRuntime.values()) {
            runtime.shutdown();
        }
        tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
        return "Success";
    }

    @Override
    public String SetTupleBatchSize(int size) {
        batch_size = size;
        return "Success";
    }

    @Override
    public String SetIntervalBetweenTuples(int interval) {
        interval_wait = interval;
        return "Success";
    }

    public String AddTuples(Map<String, Object> tuple, int quantity) {
        int stream_id = (int) tuple.get("stream-id");
        Map<String, Object> schema = allSchemas.get(stream_id);
        String stream_name = (String) schema.get("name");
        List<Map<String, Object>> attributes = (ArrayList<Map<String, Object>>) tuple.get("attributes");
        Object[] siddhi_attributes = new Object[attributes.size()];
        Attribute.Type[] streamType = (Attribute.Type[]) schema.get("stream-type");

        for (int i = 0; i < attributes.size(); i++) {
            Map<String, Object> attribute = attributes.get(i);
            siddhi_attributes[i] = attribute.get("value");
        }

        try {
            for (int i = 0; i < quantity; i++) {
                tf.traceEvent(220, new Object[]{/*event_id*/});
                Event[] events = new Event[1];
                events[0] = new Event(System.currentTimeMillis(), siddhi_attributes);
                allPackets.add(new Tuple3<>(BinaryEventConverter.convertToBinaryMessage(events, streamType).array(), streamType, stream_name));
                //eventIDs.add(event_id);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Success";
    }

    private Map<String, Object> GetMapFromYaml(Map<String, Object> ds) {
        FileInputStream fis = null;
        Yaml yaml = new Yaml();
        String dataset_path = System.getenv().get("EXPOSE_PATH") + "/" + ds.get("file");
        try {
            fis = new FileInputStream(dataset_path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return (Map<String, Object>) yaml.load(fis);
    }

    @Override
    public String SendDsAsStream(Map<String, Object> ds) {
        //System.out.println("Processing dataset");
        int ds_id = (int) ds.get("id");
        List<Map<String, Object>> tuples = datasetIdToTuples.get(ds_id);
        if (tuples == null) {
            Map<String, Object> map = GetMapFromYaml(ds);
            List<Map<String, Object>> raw_tuples = (List<Map<String, Object>>) map.get("cepevents");
            Map<Integer, List<Map<String, Object>>> ordered_tuples = new HashMap<>();
            //int i = 0;
            // Add order to tuples and place them in ordered_tuples
            for (Map<String, Object> tuple : raw_tuples) {
                //tuple.put("_order", i++);
                int tuple_stream_id = (int) tuple.get("stream-id");
                if (ordered_tuples.get(tuple_stream_id) == null) {
                    ordered_tuples.put(tuple_stream_id, new ArrayList<>());
                }
                ordered_tuples.get(tuple_stream_id).add(tuple);
            }

            // Fix the type of the tuples in ordered_tuples
            for (int stream_id : ordered_tuples.keySet()) {
                Map<String, Object> schema = allSchemas.get(stream_id);
                CastTuplesCorrectTypes(ordered_tuples.get(stream_id), schema);
            }

            // Sort the raw_tuples by their order
			/*raw_tuples.sort((lhs, rhs) -> {
				int lhs_order = (int) lhs.get("_order");
				int rhs_order = (int) rhs.get("_order");
				return Integer.compare(lhs_order, rhs_order);
			});*/

            datasetIdToTuples.put(ds_id, raw_tuples);
            tuples = raw_tuples;
        }
		/*double prevTimestamp = 0;
		//System.out.println("Ready to transmit tuples");
		long prevTime = System.nanoTime();
		boolean realism = (boolean) ds.getOrDefault("realism", false) && schema.containsKey("rowtime-column");
		for (Map<String, Object> tuple : tuples) {
			AddTuples(tuple, 1);

			if (realism) {
				Map<String, Object> rowtime_column = (Map<String, Object>) schema.get("rowtime-column");
				double timestamp = 0;
				for (Map<String, Object> attribute : (List<Map<String, Object>>) tuple.get("attributes")) {
					if (attribute.get("name").equals(rowtime_column.get("column"))) {
						int nanoseconds_per_tick = (int) rowtime_column.get("nanoseconds-per-tick");
						timestamp = (double) attribute.get("value") * nanoseconds_per_tick;
						if (prevTimestamp == 0) {
							prevTimestamp = timestamp;
						}
						break;
					}
				}
				double time_diff_tuple = timestamp - prevTimestamp;
				long time_diff_real = System.nanoTime() - prevTime;
				while (time_diff_real < time_diff_tuple) {
					time_diff_real = System.nanoTime() - prevTime;
				}

				prevTimestamp = timestamp;
				prevTime = System.nanoTime();
			}
		}

		if (!realism) {
			ProcessTuples(tuples.size());
		}*/

        for (Map<String, Object> tuple : tuples) {
            AddTuples(tuple, 1);
        }
        ProcessTuples(tuples.size());
        return "Success";
    }

    void CastTuplesCorrectTypes(List<Map<String, Object>> tuples, Map<String, Object> schema) {
        List<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
        for (Map<String, Object> tuple : tuples) {
            List<Map<String, Object>> attributes = (List<Map<String, Object>>) tuple.get("attributes");
            for (int i = 0; i < tuple_format.size(); i++) {
                Map<String, String> attribute_format = tuple_format.get(i);
                Map<String, Object> attribute = attributes.get(i);
                switch (attribute_format.get("type")) {
                    case "string":
                        attribute.put("value", attribute.get("value").toString());
                        break;
                    case "bool":
                        attribute.put("value", Boolean.valueOf(attribute.get("value").toString()));
                        break;
                    case "int":
                        attribute.put("value", Integer.parseInt(attribute.get("value").toString()));
                        break;
                    case "float":
                        attribute.put("value", Float.parseFloat(attribute.get("value").toString()));
                        break;
                    case "double":
                        attribute.put("value", Double.parseDouble(attribute.get("value").toString()));
                        break;
                    case "long":
                        attribute.put("value", Long.parseLong(attribute.get("value").toString()));
                        break;
                    case "long-timestamp":
                        // I don't know how to add external timestamp
                        attribute.put("value", Long.parseLong(attribute.get("value").toString()));
                        break;
                    case "timestamp":
                        // I don't know how to add external timestamp
                        attribute.put("value", attribute.get("value").toString());
                        break;
                    default:
                        throw new RuntimeException("Invalid attribute type in dataset definition");
                }
            }
        }
    }

    /*private List<Map<String, Object>> readTuplesFromDataset(Map<String, Object> ds) {
        int ds_id = (int) ds.get("id");
        List<Map<String, Object>> tuples = datasetIdToTuples.get(ds_id);
        if (tuples == null) {
            FileInputStream fis = null;
            Yaml yaml = new Yaml();
            try {
                String dataset_path = System.getenv().get("EXPOSE_PATH") + "/" + ds.get("file");
                fis = new FileInputStream(dataset_path);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            Map<String, Object> map = (Map<String, Object>) yaml.load(fis);
            tuples = (ArrayList<Map<String, Object>>) map.get("cepevents");
            CastTuplesCorrectTypes(tuples, schema);
            datasetIdToTuples.put(stream_id, tuples);
        }
        return tuples;
    }*/

    @Override
    public String AddNextHop(int streamId, int nodeId) {
        if (!streamIdToNodeIds.containsKey(streamId)) {
            streamIdToNodeIds.put(streamId, new ArrayList<>());
        }
        streamIdToNodeIds.get(streamId).add(nodeId);
        return "Success";
    }

    @Override
    public String WriteStreamToCsv(int stream_id, String csv_folder) {
        int cnt = 1;
        boolean finished = false;
        while (!finished) {
            String path = System.getenv().get("EXPOSE_PATH") + "/" + csv_folder + "/siddhi/" + cnt;
            Path p = Paths.get(path);
            if (Files.exists(p)) {
                ++cnt;
                continue;
            }
            File f = new File(path);
            if (!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
            try {
                f.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            FileWriter fw = null;
            try {
                fw = new FileWriter(f);
            } catch (IOException e) {
                e.printStackTrace();
            }
            BufferedWriter bw = new BufferedWriter(fw);
            streamIdToCsvWriter.put(stream_id, bw);
            finished = true;
        }
        return "Success";
    }

    @Override
    public String SetNidToAddress(Map<Integer, Map<String, Object>> newNodeIdToIpAndPort) {
        for (int node_id : newNodeIdToIpAndPort.keySet()) {
            if (nodeIdToIpAndPort.containsKey(node_id) || node_id == this.node_id) {
                continue;
            }

            String ip = (String) newNodeIdToIpAndPort.get(node_id).get("ip");
            int port = (int) newNodeIdToIpAndPort.get(node_id).get("spe-coordinator-port");
            // Establish coordinator connection with node
            try {
                this.speComm.ConnectToSpeCoordinator(node_id, ip, port);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed to connect to Node " + node_id + "'s SPE coordinator");
                System.exit(20);
            }
        }
        nodeIdToIpAndPort = newNodeIdToIpAndPort;
        return "Success";
    }

    public void ProcessTuple(int stream_id, String stream_name, Attribute.Type[] streamTypes, Event event) {
        if (!streamIdActive.getOrDefault(stream_id, false)) {
            if (streamIdBuffer.getOrDefault(stream_id, false)) {
                streamIdToBuffer.computeIfAbsent(stream_id, k -> new ArrayList<>());
                streamIdToBuffer.get(stream_id).add(new Tuple3<>(stream_name, streamTypes, event));
            }
            return;
        }
        if (streamIdToNodeIds.containsKey(streamNameToId.get(stream_name))) {
            for (Integer nodeId : streamIdToNodeIds.get(stream_id)) {
                TCPNettyClient tcpNettyClient = nodeIdToClient.get(nodeId);
                if (tcpNettyClient == null) {
                    tcpNettyClient = new TCPNettyClient(true, true);
                    nodeIdToClient.put(nodeId, tcpNettyClient);
                    for (Integer nid : nodeIdToIpAndPort.keySet()) {
                        if (nodeId.equals(nid)) {
                            Map<String, Object> addrAndPort = nodeIdToIpAndPort.get(nid);
                            tcpNettyClient.connect((String) addrAndPort.get("ip"), (int) addrAndPort.get("client-port"));
                            break;
                        }
                    }
                }

                List<Event> to_send = new ArrayList<>();
                to_send.add(event);
                try {
                    tcpNettyClient.send(stream_name, BinaryEventConverter.convertToBinaryMessage(
                            to_send.toArray(new Event[1]), streamTypes).array()).await();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    System.exit(6);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(7);
                }
            }
        }
    }

    int cnt2 = 0;
    int cnt3 = 0;
    @Override
    public String AddSchemas(List<Map<String, Object>> schemas) {
        for (Map<String, Object> schema : schemas) {
            String stream_name = (String) schema.get("name");
            int stream_id = (int) schema.get("stream-id");
            streamNameToId.put(stream_name, stream_id);
            // Create own schema based in the loop below instead of using "siddhi-specific" schema.
            allSchemas.put(stream_id, schema);
            streamIdActive.put(stream_id, true);

            StringBuilder siddhi_schema = new StringBuilder("define stream " + stream_name + " (");
            StreamDefinition streamDefinition = StreamDefinition.id(stream_name);
            for (Map<String, Object> j : (ArrayList<Map<String, Object>>) schema.get("tuple-format")) {
                siddhi_schema.append(j.get("name"));
                siddhi_schema.append(" ");
                Attribute.Type type;
                if (j.get("type").equals("string")) {
                    type = Attribute.Type.STRING;
                    siddhi_schema.append("string, ");
                } else if (j.get("type").equals("bool")) {
                    type = Attribute.Type.BOOL;
                    siddhi_schema.append("bool, ");
                } else if (j.get("type").equals("int")) {
                    type = Attribute.Type.INT;
                    siddhi_schema.append("int, ");
                } else if (j.get("type").equals("float")) {
                    type = Attribute.Type.DOUBLE;
                    siddhi_schema.append("double, ");
                } else if (j.get("type").equals("double")) {
                    type = Attribute.Type.DOUBLE;
                    siddhi_schema.append("double, ");
                } else if (j.get("type").equals("long")) {
                    type = Attribute.Type.LONG;
                    siddhi_schema.append("long, ");
                } else if (j.get("type").equals("number")) {
                    type = Attribute.Type.DOUBLE;
                    siddhi_schema.append("double, ");
                } else if (j.get("type").equals("timestamp")) {
                    type = Attribute.Type.STRING;
                    siddhi_schema.append("string, ");
                } else if (j.get("type").equals("long-timestamp")) {
                    type = Attribute.Type.LONG;
                    siddhi_schema.append("long, ");
                } else {
                    throw new RuntimeException("Invalid attribute type in stream schema");
                }
                streamDefinition = streamDefinition.attribute((String) j.get("name"), type);
            }
            // We remove the final comma and space
            siddhi_schema.setLength(siddhi_schema.length()-2);
            siddhi_schema.append(");\n");
            siddhiSchemas.put(stream_id, siddhi_schema.toString());

            Attribute.Type[] streamTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                    streamDefinition.getAttributeList());
            schema.put("stream-type", streamTypes);

            // Add stream listeners for the distributed scenario
            StreamDefinition finalStreamDefinition = streamDefinition;
            StreamListener sl = new StreamListener() {
                @Override
                public String getChannelId() {
                    return finalStreamDefinition.getId();
                }

                @Override
                public void onMessage(byte[] message) {
                    onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), streamTypes));
                }

                public void onEvents(Event[] events) {
                    for (Event event : events) {
                        onEvent(event);
                    }
                }

                public void onEvent(Event event) {
                    //LOG.info(event);
                    BufferedWriter writer = streamIdToCsvWriter.get(stream_id);
                    if (writer != null) {
                        try {
                            writer.write(event.toString() + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    timeLastRecvdTuple = System.currentTimeMillis();
                    //if (++cnt2 % 100000 == 0)
                    //System.out.println("Received event number " + (++cnt2) + ": " + event);
                    try {
                        tf.traceEvent(1, new Object[]{Thread.currentThread().getId(), Event.running_id + 1/*, eventIDs.get((int)curPktsPublished%allPackets.size())*/});
                        for (SiddhiAppRuntime runtime : queryIdToSiddhiAppRuntime.values()) {
                            runtime.getInputHandler(finalStreamDefinition.getId()).send(event);
                        }
                        tf.traceEvent(100, new Object[]{Thread.currentThread().getId(), Event.running_id/*, eventIDs.get((int)curPktsPublished%allPackets.size())*/});
                        //ProcessTuple(stream_id, finalStreamDefinition.getId(), streamTypes, event);
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            };

            this.streamListeners.add(sl);
            this.tcpNettyServer.addStreamListener(sl);

            StreamCallback sc = new StreamCallback() {
                int eventCount = 0;

                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        cnt3 += events.length;
                        eventCount++;
                        if (cnt3 % 100000 == 0)
                            System.out.println("Produced complex event " + event.GetId() + " to stream " + event.toString());
                        tf.traceEvent(6, new Object[]{Thread.currentThread().getId()});
                        ProcessTuple(stream_id, stream_name, streamTypes, event);
                    }
                }
            };
            allCallbacks.add(new Tuple2<>(stream_name, sc));
        }

        StartSiddhiAppRuntime();
        return "Success";
    }

    private void StartSiddhiAppRuntime() {
        StringBuilder schemasString = new StringBuilder();
        for (String siddhiSchema : siddhiSchemas.values()) {
            schemasString.append(siddhiSchema);
        }
        for (int query_id : queryIdToMapQuery.keySet()) {
            Map<String, Object> map_query = queryIdToMapQuery.get(query_id);
            String query = (String) ((Map<String, Object>) map_query.get("sql-query")).get("siddhi");
            queryIdToSiddhiAppRuntime.put(query_id, siddhiManager.createSiddhiAppRuntime(schemasString.toString() + "\n" + query));
            for (Tuple2<String, StreamCallback> t : allCallbacks) {
                queryIdToSiddhiAppRuntime.get(query_id).addCallback(t.getFirst(), t.getSecond());
            }
            queryIdToSiddhiAppRuntime.get(query_id).start();
        }

        if (queryIdToSiddhiAppRuntime.isEmpty()) {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(schemasString.toString());
            for (Tuple2<String, StreamCallback> t : allCallbacks) {
                siddhiAppRuntime.addCallback(t.getFirst(), t.getSecond());
            }
            siddhiAppRuntime.start();
        }
    }

    @Override
    public String DeployQueries(Map<String, Object> map_query) {
        String query = (String) ((Map<String, Object>) map_query.get("sql-query")).get("siddhi");
        if (query == null || query.equals("")) {
            return "Empty query";
        }
        int query_id = (int) map_query.get("id");
        queryIdToMapQuery.put(query_id, map_query);
        //for (int i = 0; i < quantity; i++) {
        tf.traceEvent(221, new Object[]{query_id});
        queries.append(query).append("\n");
        //}

        return "Success";
    }

    public String ProcessTuples(int number_tuples) {
        //System.out.println("Processing tuples " + (++cnt));
        if (allPackets.isEmpty()) {
            System.out.println("No tuples to process");
        }
        // Invoke all threads for this loop. Perhaps they have to wait until I wait a few milliseconds and unlock a mutex
        for (int i = 0; i < number_threads; i++) {
            //new Thread(() -> {
            while (pktsPublished < number_tuples) {
                if (allPackets.isEmpty()) {
                    break;
                }
                int curPktsPublished = pktsPublished;
                Tuple3<byte[], Attribute.Type[], String> t = allPackets.get(curPktsPublished % allPackets.size());
                ++pktsPublished;
                // Event.running_id+1 becomes the ID of the event that is created
                // We record the thread ID, running event Id and the base event Id
                Event[] events;
                events = SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(t.getFirst()), t.getSecond());
                for (Event event : events) {
                    int stream_id = streamNameToId.get(t.getThird());
                    ProcessTuple(stream_id, t.getThird(), t.getSecond(), event);
                }
            }
        }
        pktsPublished = 0;
        return "Success";
    }

    @Override
    public String ClearQueries() {
        queries.setLength(0);
        tf.traceEvent(222);
        return "Success";
    }

    @Override
    public String EndExperiment() {
        tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
        for (BufferedWriter w : streamIdToCsvWriter.values()) {
            try {
                w.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "Success";
    }

    @Override
    public String AddTpIds(List<Object> tracepointIds) {
        for (int tracepointId : (List<Integer>) (List<?>) tracepointIds) {
            this.tf.addTracepoint(tracepointId);
        }
        return "Success";
    }

    @Override
    public String RetEndOfStream(int milliseconds) {
        long time_diff;
        do {
            try {
                Thread.sleep(milliseconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            time_diff = System.currentTimeMillis() - timeLastRecvdTuple;
            System.out.println("RetEndOfStream, time_diff: " + time_diff);
        } while (time_diff < milliseconds || timeLastRecvdTuple == 0);
        return Long.toString(time_diff);
    }

    @Override
    public String TraceTuple(int tracepointId, List<String> traceArguments) {
        tf.traceEvent(tracepointId, traceArguments.toArray());
        return "Success";
    }

    @Override
    public String MoveQueryState(int query_id, int new_host) {
        byte[] snapshot = queryIdToSiddhiAppRuntime.get(query_id).snapshot();
        Map<String, Object> task = new HashMap<>();
        task.put("task", "loadQueryState");
        List<Object> task_args = new ArrayList<>();
        task_args.add(snapshot);
        Map<String, Object> map_query = queryIdToMapQuery.get(query_id);
        task_args.add(map_query);
        task.put("arguments", task_args);
        task.put("node", new_host);
        speComm.speCoordinatorComm.SendToSpe(task);
        return "Success";
    }

    @Override
    public String MoveStaticQueryState(int query_id, int new_host) {
        return "Success";
    }

    @Override
    public String MoveDynamicQueryState(int query_id, int new_host) {
        return "Success";
    }

    public String LoadQueryState(byte[] snapshot, Map<String, Object> map_query) {
        DeployQueries(map_query);
        int query_id = (int) map_query.get("id");
        String query = (String) ((Map<String, Object>) map_query.get("sql-query")).get("siddhi");
        try {
            StringBuilder schemasString = new StringBuilder();
            for (String siddhiSchema : siddhiSchemas.values()) {
                schemasString.append(siddhiSchema);
            }
            queryIdToSiddhiAppRuntime.put(query_id, siddhiManager.createSiddhiAppRuntime(schemasString.toString() + "\n" + query));
            for (Tuple2<String, StreamCallback> t : allCallbacks) {
                queryIdToSiddhiAppRuntime.get(query_id).addCallback(t.getFirst(), t.getSecond());
            }
            queryIdToSiddhiAppRuntime.get(query_id).start();
            queryIdToSiddhiAppRuntime.get(query_id).restore(snapshot);
        } catch (CannotRestoreSiddhiAppStateException e) {
            e.printStackTrace();
            System.exit(21);
        }
        return "Success";
    }

    public void FlushBuffer(int stream_id) {
        if (streamIdBuffer.getOrDefault(stream_id, false)) {
            // Flush buffer
            for (Tuple3<String, Attribute.Type[], Event> tuple : streamIdToBuffer.getOrDefault(stream_id, new ArrayList<>())) {
                String stream_name = tuple.getFirst();
                Attribute.Type[] streamTypes = tuple.getSecond();
                Event event = tuple.getThird();
                ProcessTuple(stream_id, stream_name, streamTypes, event);
            }
            streamIdToBuffer.getOrDefault(stream_id, new ArrayList<>()).clear();
        }
    }

    @Override
    public String ResumeStream(int stream_id) {
        streamIdActive.put(stream_id, true);
        FlushBuffer(stream_id);
        streamIdBuffer.put(stream_id, false);
        return "Success";
    }

    @Override
    public String StopStream(int stream_id) {
        streamIdActive.put(stream_id, false);
        return "Success";
    }

    @Override
    public String BufferStream(int stream_id) {
        streamIdBuffer.put(stream_id, true);
        return "Success";
    }

    @Override
    public String BufferAndStopStream(int stream_id) {
        BufferStream(stream_id);
        StopStream(stream_id);
        return "Success";
    }

    @Override
    public String BufferStopAndRelayStream(int stream_id, int old_host, int new_host) {
        BufferStream(stream_id);
        StopStream(stream_id);
        RelayStream(stream_id, old_host, new_host);
        return "Success";
    }

    @Override
    public String RelayStream(int stream_id, int old_host, int new_host) {
        RemoveNextHop(stream_id, old_host);
        AddNextHop(stream_id, new_host);
        return "Success";
    }

    @Override
    public String RemoveNextHop(int stream_id, int host) {
        for (int i = 0; i < streamIdToNodeIds.get(stream_id).size(); i++) {
            if (streamIdToNodeIds.get(stream_id).get(i) == host) {
                streamIdToNodeIds.get(stream_id).remove(i);
                break;
            }
        }
        return "Success";
    }

    @Override
    public String AddSourceNodes(int query_id, int stream_id, List<Integer> node_id_list) {
        Tuple2<Integer, Integer> key = new Tuple2(stream_id, query_id);
        List<Integer> value = this.streamIdAndQueryIdToSourceNodes.getOrDefault(key, new ArrayList<>());
        value.addAll(node_id_list);
        streamIdAndQueryIdToSourceNodes.put(key, value);
        return "Success";
    }

    @Override
    public String Configure() {
        siddhiManager.setExtension("udf:doltoeur", DOLTOEURFunction.class);
        return "Success";
    }

    @Override
    public void HandleSpeSpecificTask(Map<String, Object> task) {
        String cmd = (String) task.get("task");
        if (cmd.equals("loadQueryState")) {
            List<Object> args = (List<Object>) task.get("arguments");
            byte[] snapshot = (byte[]) args.get(0);
            Map<String, Object> query = (Map<String, Object>) args.get(1);
            LoadQueryState(snapshot, query);
            return;
        } else {
            throw new RuntimeException("Invalid task from mediator: " + cmd);
        }
    }

    public static void main(String[] args) {
        boolean continue_running = true;
        while (continue_running) {
            SiddhiExperimentFramework siddhiExperimentFramework = new SiddhiExperimentFramework();
            SpeComm speComm = new SpeComm(args, siddhiExperimentFramework, siddhiExperimentFramework);
            siddhiExperimentFramework.speComm = speComm;
            siddhiExperimentFramework.SetNodeId(speComm.GetNodeId());
            siddhiExperimentFramework.SetupClientTcpServer(speComm.GetClientPort());
            siddhiExperimentFramework.SetTraceOutputFolder(speComm.GetTraceOutputFolder());
            speComm.AcceptTasks();
            siddhiExperimentFramework.TearDownTcpServer();
        }
    }
}
