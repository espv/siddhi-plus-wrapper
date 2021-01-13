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
import no.uio.ifi.*;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.Math.abs;

@SuppressWarnings("unchecked")
public class SiddhiExperimentFramework implements ExperimentAPI, SpeSpecificAPI {
    private volatile int totalEventCount = 0;
    private volatile int threadCnt = 0;
    private int batch_size = 1;
    private int interval_wait;
    private int pktsPublished;
    private TracingFramework tf = new TracingFramework();
    int number_threads = 1;
    long timeLastRecvdTuple = 0;
    long MAX_OUTPUT_BUFFER_SIZE = 10;
    Map<Integer, SiddhiAppRuntime> queryIdToSiddhiAppRuntime = new HashMap<>();
    SiddhiAppRuntime siddhiAppRuntime;
    Map<Integer, Boolean> streamIdActive = new HashMap<>();
    Map<Integer, Boolean> streamIdBuffer = new HashMap<>();
    SpeTaskHandler speComm;
    private static final Logger log = Logger.getLogger(SiddhiExperimentFramework.class);

    TCPNettyServer tcpNettyServer;
    List<StreamListener> streamListeners = new ArrayList<>();
    Map<Integer, TCPNettyClient> nodeIdToClient = new HashMap<>();
    Map<String, Integer> streamNameToId = new HashMap<>();
    Map<Integer, String> streamIdToName = new HashMap<>();
    Map<Integer, Map<String, Object>> nodeIdToIpAndPort = new HashMap<>();
    Map<Integer, List<Map<String, Object>>> datasetIdToTuples = new HashMap<>();

    ArrayList<Tuple3<byte[], Attribute.Type[], String>> allPackets = new ArrayList<>();
    Map<Integer, StreamCallback> streamIdToStreamCallbacks = new HashMap<>();
    int actually_sent_tuples = 0;
    int tried_to_send_tuples = 0;
    long millisecond100Offset = System.currentTimeMillis() / 100;

    SiddhiManager siddhiManager = new SiddhiManager();
    StringBuilder queries = new StringBuilder();
    Map<Integer, Map<String, Object>> allSchemas = new HashMap<>();
    Map<Integer, String> siddhiSchemas = new HashMap<>();
    Map<Integer, List<Integer>> streamIdToNodeIds = new HashMap<>();
    Map<Integer, BufferedWriter> streamIdToCsvWriter = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> queryIdToStreamIdToNodeIds = new HashMap<>();
    Map<Integer, Map<String, Object>> queryIdToMapQuery = new HashMap<>();
    Map<Integer, Integer> queryIdToOutputStreamId = new HashMap<>();
    Map<Integer, Integer> nodeIdToTuplesPerSecondLimit = new HashMap<>();
    Map<Integer, Long> nodeIdToTupleInterval = new HashMap<>();
    Map<Integer, List<Long>> nodeIdToTimestampsTuplesSent = new HashMap<>();
    final Map<Integer, Map<Long, Long>> nodeIdToMillisecondToForwarded = new HashMap<>();
    Map<Integer, List<Long>> nodeIdToTimestampsTuplesDropped = new HashMap<>();
    Map<Integer, List<Integer>> nodeIdToStoppedStreams = new HashMap<>();
    List<Tuple2<Integer, Event>> incomingTupleBuffer = new ArrayList<>();
    List<Tuple2<Integer, byte[]>> outgoingTupleBuffer = new ArrayList<>();
    Map<Integer, List<Tuple2<Integer, byte[]>>> nodeIdToOutgoingTupleBuffer = new HashMap<>();
    Map<Integer, List<Tuple3<Long, Double, Long>>> send_schedule = new HashMap<>();

    boolean is_potential_host = false;
    List<Integer> potential_host_stream_ids = new ArrayList<>();

    int port;
    int node_id;
    private String trace_output_folder;

    boolean execution_locked = false;

    ServerSocket state_transfer_server = null;

    public void setupStateTransferServer(int state_transfer_port) {
        try {
            state_transfer_server = new ServerSocket(state_transfer_port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //@Override
    public String SetupClientTcpServer(int port) {
        this.tcpNettyServer = new TCPNettyServer();
        this.port = port;
        ServerConfig sc = new ServerConfig();
        sc.setPort(port);
        tcpNettyServer.start(sc);
        log.setLevel(Level.DEBUG);
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
    public void LockExecution() {
        execution_locked = true;
    }

    @Override
    public void UnlockExecution() {
        execution_locked = false;
    }

    public boolean IsExecutionLocked() {
        return execution_locked;
    }

    @Override
    public String StartRuntimeEnv() {
        timeLastRecvdTuple = 0;
        received_tuples = 0;
        actually_sent_tuples = 0;
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

    public void AddToSendSchedule(int node_id, long downtime, Double tuples_per_second, long current_on, int step) {
        long next_start = System.currentTimeMillis() + downtime;
        Long next_stop = next_start + current_on + step;
        Tuple3<Long, Double, Long> next_schedule = new Tuple3<>(next_start, tuples_per_second, next_stop);
        send_schedule.put(node_id, new ArrayList<>());
        send_schedule.get(node_id).add(next_schedule);
    }

    /**
     * Method sends tuples with tuples_per_second
     * @param desired_tuples_per_second
     * @param downtime
     * @param min
     * @param max
     * @param step
     */
    public void SendTuplesVariableOnOff(int desired_tuples_per_second, int downtime, int min, int max, int step) {
        long desired_ns_between_tuples = (int) ((1.0/desired_tuples_per_second) * 1000000000);
        //System.out.println("SendTuplesVariableOnOff: Node " + to_node + ", desired ns tuple interval: " + desired_ns_between_tuples + ", actual ns tuple interval: " + this.nodeIdToTupleInterval.get(to_node));
        Random r = new Random();
        Map<Integer, Double> node_to_actual_tuples_per_second = new HashMap<>();
        Map<Integer, Double> node_to_drop_probability = new HashMap<>();

        if (allPackets.isEmpty()) {
            System.out.println("No tuples to process");
        }
        long last_sent = 0;

        for (int to_node : this.nodeIdToTuplesPerSecondLimit.keySet()) {
            double max_tuples_per_second = this.nodeIdToTuplesPerSecondLimit.get(to_node);
            double actual_tuples_per_second = Math.min(desired_tuples_per_second, max_tuples_per_second);

            double drop_probability = 100 * (1 - actual_tuples_per_second/desired_tuples_per_second);
            node_to_actual_tuples_per_second.put(to_node, actual_tuples_per_second);
            node_to_drop_probability.put(to_node, drop_probability);
        }

        // TODO: Add to the send_schedule. We can basically add all the scheduled times to send here
        // TODO: It's not entirely trivial to add everything, because we calculate the next tuple to
        // TODO: be sent through delta-timing. Maybe we have to do it before each break, and simply
        // TODO: require that the break is long enough, or else increase it?
        // First send schedule. The remaining ones are added before the break at the end of an iteration
        int current_on = min;
        for (int to_node : node_to_actual_tuples_per_second.keySet()) {
            AddToSendSchedule(to_node, downtime, node_to_actual_tuples_per_second.get(to_node), current_on, step);
        }

        // TODO: tuples_per_second is actually desired tuples per second, and might be higher than what
        // nodeIdToTuplesPerSecondLimit allows
        // Therefore, we must change tuples_per_second if it's too high
        // The problem is that we don't talk about node IDs before we send the tuples, which happens in the
        // next step
        // What we might need is for the next step to wait a certain amount of time after having sent a tuple,
        // so as to make up for the tuple rate.
        // Problem is that we require a lot of assumptions that way
        for (; current_on < max; current_on += step) {
            double desired_total_packets = (desired_tuples_per_second * (current_on/1000.0));
            //double actual_total_packets = (actual_tuples_per_second * (current_on/1000.0));
            //double drop_probability = 100 * (1 - actual_total_packets/desired_total_packets);
            if (allPackets.isEmpty()) {
                break;
            }

            /*System.out.println("Sending " + actual_total_packets + " over " + current_on + " milliseconds");
            if (actual_tuples_per_second == desired_tuples_per_second) {
                System.out.println("We are hitting the target of how many tuples we want to forward");
            } else {
                System.out.println("Actual tuples this round is " + actual_total_packets + ", versus desired tuples is " + desired_total_packets);
                System.out.println("We attempt to send " + desired_total_packets + ", but have a probability of " + drop_probability + "% of dropping tuples");
            }*/

            // Now, we need to send total_packets to each node that will be a recipient
            // Problem is, we don't know who will be the recipient until we look at the stream IDs
            // I don't want thread synchronization, because that messes things up royally
            // I should really add to which node I want to send the dataset
            //
            for (int curPktsPublished = 1; curPktsPublished <= desired_total_packets; curPktsPublished++) {
                Tuple3<byte[], Attribute.Type[], String> t = allPackets.get(curPktsPublished % allPackets.size());
                ++pktsPublished;
                if (pktsPublished % 10000 == 0) {
                    System.out.println("SendTuples: " + pktsPublished + " tuples");
                }
                long current_time = System.nanoTime();
                if (desired_ns_between_tuples != 0) {
                    long time_diff = current_time - last_sent;
                    while (time_diff < desired_ns_between_tuples) {
                        current_time = System.nanoTime();
                        time_diff = current_time - last_sent;
                    }
                }

                //int random_number = abs(r.nextInt() % 101);
                //boolean tuple_is_dropped = random_number <= drop_probability;
                //System.out.println("random_number: " + random_number + ", drop_probability: " + drop_probability + ", dropped: " + tuple_is_dropped);

                // Event.running_id+1 becomes the ID of the event that is created
                // We record the thread ID, running event Id and the base event Id
                Event[] events;
                events = SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(t.getFirst()), t.getSecond());

                /*long current_time_100ms = System.currentTimeMillis() / 100;
                long current_time_100ms_norm = current_time_100ms - millisecond100Offset;

                // TODO: Replace to_node here
                synchronized (this.nodeIdToMillisecondToForwarded) {
                    this.nodeIdToMillisecondToForwarded.putIfAbsent(to_node, new HashMap<>());
                    this.nodeIdToMillisecondToForwarded.get(to_node).computeIfAbsent(current_time_100ms_norm, k -> 0L);
                    this.nodeIdToMillisecondToForwarded.get(to_node).put(current_time_100ms_norm, this.nodeIdToMillisecondToForwarded.get(to_node).get(current_time_100ms_norm) + 1);
                }

                if (tuple_is_dropped) {
                    synchronized (this.nodeIdToTimestampsTuplesDropped.get(to_node)) {
                        this.nodeIdToTimestampsTuplesDropped.get(to_node).add(0, current_time);
                    }
                    continue;
                } else {
                    synchronized (this.nodeIdToTimestampsTuplesSent.get(to_node)) {
                        this.nodeIdToTimestampsTuplesSent.get(to_node).add(0, current_time);
                    }
                }*/
                for (Event event : events) {
                    List<Event> to_send = new ArrayList<>();
                    to_send.add(event);
                    final byte[] serialized_tuple;
                    try {
                        serialized_tuple = BinaryEventConverter.convertToBinaryMessage(
                                to_send.toArray(new Event[1]), t.getSecond()).array();
                    } catch (IOException e) {
                        e.printStackTrace();
                        continue;
                        //System.exit(22);
                    }
                    int stream_id = streamNameToId.get(t.getThird());
                    String stream_name = t.getThird();
                    for (int to_node : this.streamIdToNodeIds.get(stream_id)) {
                        String topicName = stream_name + "-" + to_node;
                        int random_number = abs(r.nextInt() % 101);
                        double drop_probability = node_to_drop_probability.get(to_node);
                        boolean tuple_is_dropped = random_number <= drop_probability;
                        if (tuple_is_dropped) {
                            synchronized (this.nodeIdToTimestampsTuplesDropped.get(to_node)) {
                                this.nodeIdToTimestampsTuplesDropped.get(to_node).add(0, current_time);
                            }
                            continue;
                        } else {
                            synchronized (this.nodeIdToTimestampsTuplesSent.get(to_node)) {
                                this.nodeIdToTimestampsTuplesSent.get(to_node).add(0, current_time);
                            }
                        }
                        //SendTupleToNode(to_node, stream_id, serialized_tuple);
                    }
                    //PrepareToSendTuple(stream_id, t.getSecond(), event);
                }
                last_sent = System.nanoTime();
            }

            // Do we have another iteration?
            for (int to_node : node_to_actual_tuples_per_second.keySet()) {
                AddToSendSchedule(to_node, downtime, node_to_actual_tuples_per_second.get(to_node), current_on, step);
            }

            if (downtime > 0) {
                System.out.println("Stopping for " + downtime + " milliseconds");
                try {
                    Thread.sleep(downtime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        pktsPublished = 0;
    }

    @Override
    public String SetTuplesPerSecondLimit(int tuples_per_second, List<Integer> node_list) {
        long ns_between_tuples = (int) ((1.0/tuples_per_second) * 1000000000);
        for (int node_id : node_list) {
            this.nodeIdToTuplesPerSecondLimit.put(node_id, tuples_per_second);
            this.nodeIdToTupleInterval.put(node_id, ns_between_tuples);
            System.out.println("Node tuple interval: " + ns_between_tuples);
        }
        return "Success";
    }

    @Override
    public String SendDsAsVariableOnOffStream(Map<String, Object> ds, int desired_tuples_per_second, int downtime, int min, int max, int step) {
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
                ordered_tuples.computeIfAbsent(tuple_stream_id, k -> new ArrayList<>());
                ordered_tuples.get(tuple_stream_id).add(tuple);
            }

            // Fix the type of the tuples in ordered_tuples
            for (int stream_id : ordered_tuples.keySet()) {
                Map<String, Object> schema = allSchemas.get(stream_id);
                CastTuplesCorrectTypes(ordered_tuples.get(stream_id), schema);
            }

            datasetIdToTuples.put(ds_id, raw_tuples);
            tuples = raw_tuples;
        }

        for (Map<String, Object> tuple : tuples) {
            AddTuples(tuple, 1);
        }

        new Thread(() -> SendTuplesVariableOnOff(desired_tuples_per_second, downtime, min, max, step)).start();
        boolean cont = true;
        while (cont);
        allPackets.clear();
        return "Success";
    }

    @Override
    public String SendDsAsConstantStream(Map<String, Object> ds, int desired_tuples_per_second) {
        //System.out.println("Processing dataset");
        int ds_id = (int) ds.get("id");
        List<Map<String, Object>> tuples = datasetIdToTuples.get(ds_id);
        if (tuples == null) {
            System.out.println("Before reading the dataset");
            Map<String, Object> map = GetMapFromYaml(ds);
            System.out.println("After reading the dataset");
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

            datasetIdToTuples.put(ds_id, raw_tuples);
            tuples = raw_tuples;
        }

        for (Map<String, Object> tuple : tuples) {
            AddTuples(tuple, 1);
        }

        new Thread(() -> SendTuplesVariableOnOff(desired_tuples_per_second, 0, Integer.MAX_VALUE-1, Integer.MAX_VALUE, 0)).start();
        boolean cont = true;
        while (cont);
        return "Success";
    }

    @Override
    public String SendDsAsStream(Map<String, Object> ds, int iterations, boolean realism) {
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
                ordered_tuples.computeIfAbsent(tuple_stream_id, k -> new ArrayList<>());
                ordered_tuples.get(tuple_stream_id).add(tuple);
            }

            // Fix the type of the tuples in ordered_tuples
            for (int stream_id : ordered_tuples.keySet()) {
                Map<String, Object> schema = allSchemas.get(stream_id);
                CastTuplesCorrectTypes(ordered_tuples.get(stream_id), schema);
            }

            datasetIdToTuples.put(ds_id, raw_tuples);
            tuples = raw_tuples;
        }

        for (int i = 0; i < iterations; i++) {
            for (Map<String, Object> tuple : tuples) {
                AddTuples(tuple, 1);
            }
        }

        //new Thread(() -> SendTuplesVariableOnOff(Integer.MAX_VALUE, 0, Integer.MAX_VALUE-1, Integer.MAX_VALUE, 0)).start();
        //boolean cont = true;
        //while (cont);
        SendTuples(allPackets.size());
        allPackets.clear();
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

    @Override
    public String AddNextHop(List<Integer> streamId_list, List<Integer> nodeId_list) {
        for (int streamId : streamId_list) {
            if (!streamIdToNodeIds.containsKey(streamId)) {
                streamIdToNodeIds.put(streamId, new ArrayList<>());
            }
            for (int nodeId : nodeId_list) {
                streamIdToNodeIds.get(streamId).add(nodeId);
            }
        }
        for (int nodeId : nodeId_list) {
            if (this.nodeIdToOutgoingTupleBuffer.get(nodeId) == null) {
                this.nodeIdToOutgoingTupleBuffer.put(nodeId, new ArrayList<>());
                this.nodeIdToTimestampsTuplesSent.put(nodeId, new ArrayList<>());
                this.nodeIdToMillisecondToForwarded.put(nodeId, new HashMap<>());
                this.nodeIdToTimestampsTuplesDropped.put(nodeId, new ArrayList<>());

                TCPNettyClient tcpNettyClient = nodeIdToClient.get(nodeId);
                if (tcpNettyClient == null) {
                    tcpNettyClient = new TCPNettyClient(true, true);
                    nodeIdToClient.put(nodeId, tcpNettyClient);
                    Map<String, Object> addrAndPort = nodeIdToIpAndPort.get(nodeId);
                    if (addrAndPort == null || tcpNettyClient == null) {
                        System.out.println("Error");
                    }
                    tcpNettyClient.connect((String) addrAndPort.get("ip"), (int) addrAndPort.get("client-port"));
                }
                // Start transmission thread
                final TCPNettyClient finalTcpNettyClient = tcpNettyClient;
                new Thread(() -> SendTupleToNode(finalTcpNettyClient, nodeId)).start();
            }
        }
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

    public void SendTupleToNode(TCPNettyClient tcpNettyClient, int nodeId) {
        long last_diff_surplus = 0;
        long last_sent_tuple = 0;
        while (true) {
            //System.out.println("Took " + (System.nanoTime() - last_sent_tuple) + " nanoseconds to send tuple");
            long current_time = System.nanoTime();
            // We only care about when a tuple was last sent if there is a tuple interval
            long tuple_interval = this.nodeIdToTupleInterval.getOrDefault(nodeId, 0L);
            while (nodeIdToOutgoingTupleBuffer.get(nodeId).isEmpty() || (current_time - last_sent_tuple + last_diff_surplus < tuple_interval)) {
                //Thread.yield();
                /*synchronized (this.nodeIdToOutgoingTupleBuffer.get(nodeId)) {
                    try {
                        this.nodeIdToOutgoingTupleBuffer.get(nodeId).wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }*/
                current_time = System.nanoTime();
            }
            //last_diff_surplus = max((current_time - last_sent_tuple + last_diff_surplus + 100000), 0);
            last_sent_tuple = current_time;
            Tuple2<Integer, byte[]> tuple = nodeIdToOutgoingTupleBuffer.get(nodeId).remove(0);
            if (tuple == null) {
                continue;
            }

            int stream_id = tuple.getFirst();
            byte[] serialized_tuple = tuple.getSecond();
            String stream_name = this.streamIdToName.get(stream_id);

            /*if (tuple_interval > 0) {
                if ((current_time - last_sent_tuple) + last_diff_surplus < tuple_interval) {
                    while (current_time - last_sent_tuple < tuple_interval) {
                        //Thread.yield();
                        current_time = System.nanoTime();
                    }
                    last_diff_surplus = max((current_time - last_sent_tuple), 0);
                } else {
                    last_diff_surplus = 0;
                }
                System.out.println("Time since last sent tuple: " + (current_time - last_sent_tuple) + ", tuple interval: " + tuple_interval);
                //Last sent tuple is now current_time, which will be used the next time a tuple is attempted sent
                last_sent_tuple = current_time;
                this.nodeIdToTimestampsTuplesSent.get(nodeId).add(current_time);
            }*/
            tf.traceEvent(2, new Object[]{nodeId, stream_id});
            actually_sent_tuples++;
            if (actually_sent_tuples % 10000 == 0) {
                //System.out.println("Actually sent " + actually_sent_tuples + " tuples");
                //System.out.println("Tried to send " + tried_to_send_tuples + " tuples");
            }
            try {
                tcpNettyClient.send(stream_name, serialized_tuple).await();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(6);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(7);
            }
        }
    }

    long lastTimeAddedTuple = 0;
    public void SendTuple(int stream_id, byte[] serialized_tuple) {
        String stream_name = streamIdToName.get(stream_id);
        if (streamIdToNodeIds.containsKey(stream_id)) {
            List<Integer> node_ids = streamIdToNodeIds.get(stream_id);
            for (int i = 0; i < node_ids.size(); i++) {
                int nodeId = node_ids.get(i);
                TCPNettyClient tcpNettyClient = nodeIdToClient.get(nodeId);
                if (tcpNettyClient == null) {
                    tcpNettyClient = new TCPNettyClient(true, true);
                    nodeIdToClient.put(nodeId, tcpNettyClient);
                    for (int nid : nodeIdToIpAndPort.keySet()) {
                        if (nodeId == nid) {
                            Map<String, Object> addrAndPort = nodeIdToIpAndPort.get(nid);
                            tcpNettyClient.connect((String) addrAndPort.get("ip"), (int) addrAndPort.get("client-port"));
                            break;
                        }
                    }
                }

                tf.traceEvent(2, new Object[]{nodeId, stream_id});
                //System.out.println("SendTuple stream name: " + stream_name + " to Node " + nodeId);
                try {
                    actually_sent_tuples++;
                    if (actually_sent_tuples % 10000 == 0) {
                        System.out.println("Actually sent " + actually_sent_tuples + " tuples");
                    }
                    tcpNettyClient.send(stream_name, serialized_tuple).await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(6);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(7);
                }
            }
        }
    }

    public void PrepareToSendTuple(int stream_id, Attribute.Type[] streamTypes, Event event) {
        List<Event> to_send = new ArrayList<>();
        to_send.add(event);
        byte[] serialized_tuple = null;
        try {
            serialized_tuple = BinaryEventConverter.convertToBinaryMessage(
                    to_send.toArray(new Event[1]), streamTypes).array();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(22);
        }
        if (!streamIdActive.getOrDefault(stream_id, false)) {
            if (streamIdBuffer.getOrDefault(stream_id, false)) {
                ++tried_to_send_tuples;
                outgoingTupleBuffer.add(new Tuple2(stream_id, serialized_tuple));
            }
            return;
        }

        SendTuple(stream_id, serialized_tuple);
    }

    @Override
    public String SetAsPotentialHost(List<Integer> stream_id_list) {
        this.is_potential_host = true;
        potential_host_stream_ids = stream_id_list;
        return "Success";
    }

    public void ProcessTuple(int stream_id, String stream_name, Event event) {
        try {
            tf.traceEvent(1, new Object[]{Thread.currentThread().getId(), Event.running_id + 1, stream_id});
            for (SiddhiAppRuntime runtime : queryIdToSiddhiAppRuntime.values()) {
                runtime.getInputHandler(stream_name).send(event);
            }
            tf.traceEvent(100, new Object[]{Thread.currentThread().getId(), Event.running_id, stream_id});
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    int cnt2 = 0;
    int cnt3 = 0;
    long received_tuples = 0;
    long received_tuples_3 = 0;
    @Override
    public String AddSchemas(List<Map<String, Object>> schemas) {
        for (Map<String, Object> schema : schemas) {
            String stream_name = (String) schema.get("name");
            int stream_id = (int) schema.get("stream-id");
            streamNameToId.put(stream_name, stream_id);
            streamIdToName.put(stream_id, stream_name);
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
                    //log.info(event);
                    //System.out.println("Received event " + event);
                    BufferedWriter writer = streamIdToCsvWriter.get(stream_id);
                    if (writer != null) {
                        try {
                            writer.write(event.toString() + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    timeLastRecvdTuple = System.currentTimeMillis();
                    received_tuples++;
                    //if (++cnt2 % 100000 == 0)
                    //System.out.println("Received event number " + (++cnt2) + ": " + event);

                    if ((is_potential_host && potential_host_stream_ids.contains(stream_id)) ||  !streamIdActive.getOrDefault(stream_id, true)) {
                        if (potential_host_stream_ids.contains(stream_id) || streamIdBuffer.getOrDefault(stream_id, false)) {
                            incomingTupleBuffer.add(new Tuple2(stream_id, event));
                        }
                        return;
                    }

                    if (stream_id == 0) {
                        String raw_stream_id_list = (String) event.getData()[0];
                        List<Integer> stream_id_list = new ArrayList<>();
                        for (String string_stream_id : raw_stream_id_list.split(", ")) {
                            stream_id_list.add(Integer.parseInt(string_stream_id));
                        }
                        int sending_node_id = (int) event.getData()[1];
                        if (!nodeIdToStoppedStreams.containsKey(sending_node_id)) {
                            nodeIdToStoppedStreams.put(sending_node_id, new ArrayList<>());
                        }
                        nodeIdToStoppedStreams.get(sending_node_id).addAll(stream_id_list);
                    } else {
                        ProcessTuple(stream_id, finalStreamDefinition.getId(), event);
                    }
                }
            };

            this.streamListeners.add(sl);
            this.tcpNettyServer.addStreamListener(sl);

            StreamCallback sc = new StreamCallback() {
                int eventCount = 0;

                @Override
                public void receive(Event[] events) {
                    while (IsExecutionLocked()) {
                        Thread.yield();
                    }
                    for (Event event : events) {
                        cnt3 += events.length;
                        eventCount++;
                        if (cnt3 % 100000 == 0)
                            System.out.println("Produced complex event " + event.GetId() + " to stream " + event.toString());
                        tf.traceEvent(6, new Object[]{Thread.currentThread().getId()});
                        PrepareToSendTuple(stream_id, streamTypes, event);
                    }
                }
            };
            streamIdToStreamCallbacks.put(stream_id, sc);
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
            String schemas_and_query = schemasString.toString() + "\n" + query;
            queryIdToSiddhiAppRuntime.put(query_id, siddhiManager.createSiddhiAppRuntime(schemas_and_query));

            int output_stream_id = queryIdToOutputStreamId.get(query_id);
            String output_stream_name = streamIdToName.get(output_stream_id);
            queryIdToSiddhiAppRuntime.get(query_id).addCallback(output_stream_name, streamIdToStreamCallbacks.get(output_stream_id));
            queryIdToSiddhiAppRuntime.get(query_id).start();
            //queryIdToSiddhiAppRuntime.get(query_id).setStatisticsLevel(DETAIL);
        }

        /*if (queryIdToSiddhiAppRuntime.isEmpty()) {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(schemasString.toString());
            for (int stream_id : streamIdToStreamCallbacks.keySet()) {
                String stream_name = streamIdToName.get(stream_id);
                siddhiAppRuntime.addCallback(stream_name, streamIdToStreamCallbacks.get(stream_id));
            }
            siddhiAppRuntime.start();
        }*/
    }

    @Override
    public String DeployQueries(Map<String, Object> map_query) {
        String query = (String) ((Map<String, Object>) map_query.get("sql-query")).get("siddhi");
        if (query == null || query.equals("")) {
            return "Empty query";
        }
        int query_id = (int) map_query.get("id");
        int output_stream_id = (int) map_query.get("output-stream-id");
        queryIdToOutputStreamId.put(query_id, output_stream_id);
        queryIdToMapQuery.put(query_id, map_query);
        //for (int i = 0; i < quantity; i++) {
        tf.traceEvent(221, new Object[]{query_id});
        queries.append(query).append("\n");
        //}

        return "Success";
    }

    public String SendTuples(int number_tuples) {
        // TODO: Add to send_schedule
        System.out.println("Sending " + number_tuples + " tuples");
        if (allPackets.isEmpty()) {
            System.out.println("No tuples to process");
        }

        long startTime = System.nanoTime();
        while (pktsPublished < number_tuples) {
            if (allPackets.isEmpty()) {
                break;
            }

            int curPktsPublished = pktsPublished;
            Tuple3<byte[], Attribute.Type[], String> t = allPackets.get(curPktsPublished % allPackets.size());
            ++pktsPublished;
            if (pktsPublished % 10000 == 0) {
                System.out.println("SendTuples: " + pktsPublished + " tuples");
            }
            if (interval_wait != 0 && pktsPublished % batch_size == 0) {
                // Microseconds
                long time_diff = (System.nanoTime() - startTime) / 1000;
                while (time_diff < pktsPublished * interval_wait) {
                    time_diff = (System.nanoTime() - startTime) / 1000;
                }
            }
            // Event.running_id+1 becomes the ID of the event that is created
            // We record the thread ID, running event Id and the base event Id
            Event[] events;
            events = SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(t.getFirst()), t.getSecond());
            for (Event event : events) {
                int stream_id = streamNameToId.get(t.getThird());

                PrepareToSendTuple(stream_id, t.getSecond(), event);
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
    public String RetReceivedXTuples(int number_tuples) {
        do {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("RetReceivedXTuples, number received tuples: " + received_tuples);
        } while (received_tuples < number_tuples);
        return Long.toString(number_tuples);
    }

    @Override
    public String TraceTuple(int tracepointId, List<String> traceArguments) {
        tf.traceEvent(tracepointId, traceArguments.toArray());
        return "Success";
    }

    @Override
    public String MoveQueryState(int query_id, int new_host) {
        long ms_start = System.currentTimeMillis();
        byte[] snapshot = queryIdToSiddhiAppRuntime.get(query_id).snapshot();

        new Thread(() -> {
            // Begin to set up state transfer connection
            String ip = (String) this.nodeIdToIpAndPort.get(new_host).get("ip");
            int new_host_state_transfer_port = (int) this.nodeIdToIpAndPort.get(new_host).get("state-transfer-port");
            Socket clientSocket = null;
            try {
                clientSocket = new Socket(ip, new_host_state_transfer_port);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(100);
            }
            OutputStream out = null;
            try {
                out = clientSocket.getOutputStream();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(101);
            }
            try {
                out.write(snapshot);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        // Now we have sent the snapshot to the new host
        // The new host will receive in the task how many bytes it must receive on its socket

        // If tuples are currently being processed, the snapshot will contain them.
        // If a new tuple is received between the snapshot is received above and the
        // runtime environment is shut down below, there is a lock that prevents a conflict
        // from occurring. The lock currently has flaws that need to be fixed.
        queryIdToSiddhiAppRuntime.get(query_id).shutdown();
        queryIdToSiddhiAppRuntime.remove(query_id);
        if (queryIdToSiddhiAppRuntime.isEmpty()) {
            StartSiddhiAppRuntime();
        }
        Map<String, Object> task = new HashMap<>();
        task.put("task", "loadQueryState");
        List<Object> task_args = new ArrayList<>();
        task_args.add(snapshot.length);
        Map<String, Object> map_query = queryIdToMapQuery.get(query_id);
        task_args.add(map_query);
        Map<Integer, List<Integer>> streamIdsToNodeIds = queryIdToStreamIdToNodeIds.getOrDefault(query_id, new HashMap<>());
        task_args.add(streamIdsToNodeIds);
        task.put("arguments", task_args);
        task.put("node", Collections.singletonList(new_host));
        long ms_stop1 = System.currentTimeMillis();
        speComm.speNodeComm.SendToSpe(task);
        long ms_stop2 = System.currentTimeMillis();
        System.out.println("Preparing state took " + (ms_stop1-ms_start) + "ms, and in addition to sending it took " + (ms_stop2-ms_start) + "ms");
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

    public String LoadQueryState(int snapshot_length, Map<String, Object> map_query, Map<Integer, List<Integer>> stream_ids_to_source_node_ids) {
        System.out.println("Receiving query state with " + snapshot_length + " bytes");
        long ms_start = System.currentTimeMillis();
        is_potential_host = false;
        DeployQueries(map_query);
        int query_id = (int) map_query.get("id");
        queryIdToStreamIdToNodeIds.put(query_id, stream_ids_to_source_node_ids);
        String query = (String) ((Map<String, Object>) map_query.get("sql-query")).get("siddhi");

        Socket client_socket = null;
        try {
            client_socket = state_transfer_server.accept();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(102);
        }

        byte[] snapshot = new byte[snapshot_length];
        try {
            new DataInputStream(client_socket.getInputStream()).readFully(snapshot);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(103);
        }

        try {
            StringBuilder schemasString = new StringBuilder();
            for (String siddhiSchema : siddhiSchemas.values()) {
                schemasString.append(siddhiSchema);
            }
            queryIdToSiddhiAppRuntime.put(query_id, siddhiManager.createSiddhiAppRuntime(schemasString.toString() + "\n" + query));
            int output_stream_id = queryIdToOutputStreamId.get(query_id);
            String output_stream_name = streamIdToName.get(output_stream_id);
            queryIdToSiddhiAppRuntime.get(query_id).addCallback(output_stream_name, streamIdToStreamCallbacks.get(output_stream_id));
            queryIdToSiddhiAppRuntime.get(query_id).start();
            //queryIdToSiddhiAppRuntime.get(query_id).setStatisticsLevel(DETAIL);
            queryIdToSiddhiAppRuntime.get(query_id).restore(snapshot);
        } catch (CannotRestoreSiddhiAppStateException e) {
            e.printStackTrace();
            System.exit(21);
        }

        long ms_stop1 = System.currentTimeMillis();
        ResumeStream(new ArrayList<>(stream_ids_to_source_node_ids.keySet()));
        long ms_stop2 = System.currentTimeMillis();
        System.out.println("Loading the state took " + (ms_stop1 - ms_start) + ", and in addition to resuming the streams took " + (ms_stop2 - ms_start) + "ms");
        return "Success";
    }

    public void FlushBuffer(List<Integer> stream_id_list) {
        int sent_buffered_tuples = 0;
        System.out.println("FlushBuffer: Outgoing buffer has " + outgoingTupleBuffer.size() + " tuples in it");
        for (int i = outgoingTupleBuffer.size() - 1; i >= 0; i--) {
            int buffered_tuple_stream_id = outgoingTupleBuffer.get(i).getFirst();
            byte[] serialized_tuple = outgoingTupleBuffer.get(i).getSecond();
            if (stream_id_list.contains(buffered_tuple_stream_id)) {
                if (sent_buffered_tuples % 10000 == 0) {
                    System.out.println("FlushBuffer: sent " + sent_buffered_tuples + " tuples");
                }
                SendTuple(buffered_tuple_stream_id, serialized_tuple);
                outgoingTupleBuffer.remove(i);
                ++sent_buffered_tuples;
            }
        }
        System.out.println("FlushBuffer: " + sent_buffered_tuples + " buffered tuples");

        System.out.println("Incoming buffer has " + incomingTupleBuffer.size() + " tuples in it");
        for (int i = incomingTupleBuffer.size() - 1; i >= 0; i--) {
            int buffered_tuple_stream_id = incomingTupleBuffer.get(i).getFirst();
            Event event = incomingTupleBuffer.get(i).getSecond();
            if (stream_id_list.contains(buffered_tuple_stream_id)) {
                String stream_name = streamIdToName.get(buffered_tuple_stream_id);
                ProcessTuple(buffered_tuple_stream_id, stream_name, event);
                incomingTupleBuffer.remove(i);
                ++sent_buffered_tuples;
            }
        }
        System.out.println("Processed " + sent_buffered_tuples + " buffered tuples");
    }

    @Override
    public String ResumeStream(List<Integer> stream_id_list) {
        is_potential_host = false;
        potential_host_stream_ids.clear();
        for (int stream_id : stream_id_list) {
            streamIdActive.put(stream_id, true);
            streamIdBuffer.put(stream_id, false);
        }
        System.out.println("Sent " + actually_sent_tuples + " tuples");
        FlushBuffer(stream_id_list);
        return "Success";
    }

    @Override
    public String StopStream(List<Integer> stream_id_list, int migration_coordinator_node_id) {
        for (int stream_id : stream_id_list) {
            streamIdActive.put(stream_id, false);
        }

        // Node 0 is the coordinator
        if (migration_coordinator_node_id != 0) {
            streamIdToNodeIds.put(0, Collections.singletonList(migration_coordinator_node_id));
            StringBuilder raw_stream_id_list = new StringBuilder();
            for (int stream_id : stream_id_list) {
                raw_stream_id_list.append(stream_id);
                if (stream_id != stream_id_list.get(stream_id_list.size() - 1)) {
                    raw_stream_id_list.append(", ");
                }
            }
            Attribute.Type[] streamTypes = (Attribute.Type[]) allSchemas.get(0).get("stream-type");
            Event event = new Event(System.currentTimeMillis(), new Object[]{raw_stream_id_list.toString(), node_id});
            PrepareToSendTuple(0, streamTypes, event);
            streamIdToNodeIds.remove(0);
        }
        return "Success";
    }

    @Override
    public String WaitForStoppedStreams(List<Integer> node_id_list, List<Integer> stream_id_list) {
        for (int node_id : node_id_list) {
            while (!stream_id_list.isEmpty()) {
                List<Integer> stoppedStreams = nodeIdToStoppedStreams.getOrDefault(node_id, new ArrayList<>());
                for (int i = stream_id_list.size() - 1; i >= 0; i--) {
                    int stream_id_to_stop = stream_id_list.get(i);
                    for (int stopped_stream_id : stoppedStreams) {
                        if (stream_id_to_stop == stopped_stream_id) {
                            // Remove from stream_id_list
                            stream_id_list.remove(i);
                        }
                    }
                }
            }
        }
        return "Success";
    }

    public long PredictNumberTuplesSent(int ms_ahead) {
        long number_tuples = 0;

        return number_tuples;
    }

    @Override
    public Map<String, Object> CollectMetrics(long metrics_window_ms) {
        //System.out.println("metrics window in ms: " + metrics_window_ms);
        long metrics_window_ns = metrics_window_ms * 1000000;
        double metrics_window_s = metrics_window_ms / 1000.0;
        long current_time = System.nanoTime();
        long current_time_ms = System.currentTimeMillis();
        Map<String, Object> metrics = new HashMap<>();
        Map<Integer, String> nodeIdToRegressionModels = new HashMap<>();
        metrics.put("node", this.node_id);
        Map<Integer, Object> sent_metrics = new HashMap<>();
        Map<Integer, Object> dropped_metrics = new HashMap<>();
        metrics.put("actual-sent-packets-per-second", sent_metrics);
        metrics.put("dropped-packets-per-second", dropped_metrics);
        metrics.put("forwarded-tuples-regression-models", nodeIdToRegressionModels);

        Set<Integer> allNextHopNodes = new HashSet<>();
        for (List<Integer> node_ids : streamIdToNodeIds.values()) {
            allNextHopNodes.addAll(node_ids);
        }
        long cutoff_time = current_time - metrics_window_ns;
        long current_time_100ms = (current_time_ms / 100) - this.millisecond100Offset;
        long cutoff_time_100ms = (current_time_ms - metrics_window_ms) / 100 - this.millisecond100Offset;
        for (int next_hop : allNextHopNodes) {
            int sent_pps;
            int sent_cnt = 0;
            synchronized (this.nodeIdToTimestampsTuplesSent.get(next_hop)) {
                for (long timestampSent : this.nodeIdToTimestampsTuplesSent.get(next_hop)) {
                    if (timestampSent < cutoff_time) {
                        break;
                    }
                    ++sent_cnt;
                }
            }
            sent_pps = (int) (sent_cnt / metrics_window_s);
            sent_metrics.put(next_hop, Integer.toString(sent_pps));
            int dropped_pps;
            int dropped_cnt = 0;

            synchronized (this.nodeIdToTimestampsTuplesDropped.get(next_hop)) {
                for (long timestampDropped : this.nodeIdToTimestampsTuplesDropped.get(next_hop)) {
                    if (timestampDropped < cutoff_time) {
                        break;
                    }
                    ++dropped_cnt;
                }
            }
            dropped_pps = (int) (dropped_cnt / metrics_window_s);
            dropped_metrics.put(next_hop, Integer.toString(dropped_pps));
            this.nodeIdToTimestampsTuplesSent.get(next_hop);
            this.nodeIdToTimestampsTuplesDropped.get(next_hop);
        }

        for (int node_id : this.nodeIdToMillisecondToForwarded.keySet()) {
            SimpleRegression regression = new SimpleRegression();
            Map<Long, Long> millisecondsToForwarded = this.nodeIdToMillisecondToForwarded.get(node_id);
            for (long i = 0; i <= current_time_100ms; i++) {
                synchronized (this.nodeIdToMillisecondToForwarded.get(node_id)) {
                    this.nodeIdToMillisecondToForwarded.get(node_id).computeIfAbsent(i, k -> 0L);
                }
            }
            for (long ms : millisecondsToForwarded.keySet()) {
                if (ms < cutoff_time_100ms) {
                    continue;
                }
                long numberForwarded = millisecondsToForwarded.get(ms);
                System.out.println("Number forwarded at " + ms + ": " + numberForwarded + ", cutoff_time_100ms: " + cutoff_time_100ms);
                regression.addData(ms, numberForwarded);
            }
            System.out.println("Regression slope for Node " + node_id + ": " + regression.getSlope());

            for (int i = 0; i < metrics_window_ms/500; i++) {
                System.out.println(i*100 + " milliseconds ahead (" + (current_time_100ms + i) + "): " + Math.max(0, Math.floor(regression.predict(current_time_100ms + i))) + " tuples forwarded");
            }
        }

        return metrics;
    }

    @Override
    public String BufferStream(List<Integer> stream_id_list) {
        for (int stream_id : stream_id_list) {
            streamIdBuffer.put(stream_id, true);
        }
        return "Success";
    }

    @Override
    public String RelayStream(List<Integer> stream_id_list, List<Integer> old_host_list, List<Integer> new_host_list) {
        RemoveNextHop(stream_id_list, old_host_list);
        AddNextHop(stream_id_list, new_host_list);
        return "Success";
    }

    @Override
    public String RemoveNextHop(List<Integer> stream_id_list, List<Integer> host_list) {
        for (int stream_id : stream_id_list) {
            for (int i = 0; i < streamIdToNodeIds.get(stream_id).size(); i++) {
                for (int host : host_list) {
                    if (streamIdToNodeIds.get(stream_id).get(i) == host) {
                        streamIdToNodeIds.get(stream_id).remove(i);
                        break;
                    }
                }
            }
        }
        return "Success";
    }

    @Override
    public String AddSourceNodes(int query_id, List<Integer> stream_id_list, List<Integer> node_id_list) {
        if (!queryIdToStreamIdToNodeIds.containsKey(query_id)) {
            queryIdToStreamIdToNodeIds.put(query_id, new HashMap<>());
        }

        Map<Integer, List<Integer>> streamIdsToSourceNodeIds = queryIdToStreamIdToNodeIds.get(query_id);
        for (int stream_id : stream_id_list) {
            List<Integer> value = streamIdsToSourceNodeIds.getOrDefault(stream_id, new ArrayList<>());
            value.addAll(node_id_list);
            streamIdsToSourceNodeIds.put(stream_id, value);
        }
        return "Success";
    }

    @Override
    public String Configure() {
        siddhiManager.setExtension("udf:doltoeur", DOLTOEURFunction.class);
        return "Success";
    }

    @Override
    public String Wait(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "Success";
    }

    @Override
    public void HandleSpeSpecificTask(Map<String, Object> task) {
        String cmd = (String) task.get("task");
        switch (cmd) {
            case "loadQueryState": {
                List<Object> args = (List<Object>) task.get("arguments");
                int snapshot_length = (int) args.get(0);
                Map<String, Object> query = (Map<String, Object>) args.get(1);
                Map<Integer, List<Integer>> stream_ids_to_node_ids = (Map<Integer, List<Integer>>) args.get(2);
                LoadQueryState(snapshot_length, query, stream_ids_to_node_ids);
                break;
            } default:
                throw new RuntimeException("Invalid task from mediator: " + cmd);
        }
    }

    public static void main(String[] args) {
        boolean continue_running = true;
        while (continue_running) {
            SiddhiExperimentFramework siddhiExperimentFramework = new SiddhiExperimentFramework();
            SpeTaskHandler speComm = new SpeTaskHandler(args, siddhiExperimentFramework, siddhiExperimentFramework);
            siddhiExperimentFramework.speComm = speComm;
            siddhiExperimentFramework.SetNodeId(speComm.GetNodeId());
            siddhiExperimentFramework.SetupClientTcpServer(speComm.GetClientPort());
            siddhiExperimentFramework.SetTraceOutputFolder(speComm.GetTraceOutputFolder());
            int state_transfer_port = speComm.GetStateTransferPort();
            if (state_transfer_port != -1) {
                siddhiExperimentFramework.setupStateTransferServer(state_transfer_port);
            }
            //speComm.AcceptTasks();
            while (continue_running);
            siddhiExperimentFramework.TearDownTcpServer();
        }
    }
}
