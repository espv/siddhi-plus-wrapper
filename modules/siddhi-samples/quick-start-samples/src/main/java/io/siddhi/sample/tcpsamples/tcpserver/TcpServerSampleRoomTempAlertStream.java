package io.siddhi.sample.tcpsamples.tcpserver;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyServer;
import org.wso2.extension.siddhi.io.tcp.transport.callback.StreamListener;
import org.wso2.extension.siddhi.io.tcp.transport.config.ServerConfig;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

public class TcpServerSampleRoomTempAlertStream {
    private static final Logger LOG = Logger.getLogger(TcpServerSampleRoomTempAlertStream.class);
    private static boolean keepRunning = true;
    private static TCPNettyClient tcpNettyClient;
    private static volatile Semaphore s = new Semaphore(0);
    private static final List<InputHandlerEventTuple> eventQueue = Collections.synchronizedList(new ArrayList<>());
    private static Object LOCK = new Object();

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("USAGE: java quick-start-samples.jar -classpath io.siddhi.sample.tcpsamples.tcpserver.TcpServerSample <sink_ip_address> <number_threads>");
            System.exit(0);
        }

        String ip = args[0];

        int numberThreads = Integer.parseInt(args[1]);
        tcpNettyClient = new TCPNettyClient(true, true);
        try {
            tcpNettyClient.connect(ip, 9893);
        } catch (ConnectionUnavailableException e) {
            e.printStackTrace();
        }
        /*// We create one too few Threads (numberThreads-1) because the main thread will also call Run.
        // This way, we avoid redundant threads going on.
        for (int i = 0; i < numberThreads-1; i++) {
            new Thread(TcpServerSampleTwoStateRule::Run).start();
        }*/

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                " define stream RoomTemperatureAlertStream (roomNo string, initialTemp double, finalTemp double); " +
                " " +
                " define stream RoomTemperatureStream (roomNo string, temp double); ";
        String query = "" +
                " @info(name = 'query1') " +
                " from every e1 = RoomTemperatureStream -> e2 = RoomTemperatureStream[e1.roomNo == roomNo] " +
                " select e1.roomNo as roomNo, e1.temp as initialTemp, e2.temp as finalTemp " +
                " insert into RoomTemperatureAlertStream; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.start();
        InputHandler rsInputHandler = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");

        final StreamDefinition roomStreamDefinition = StreamDefinition.id("RoomTemperatureStream").attribute("roomNo", Attribute.Type.STRING)
                .attribute("initialTemp", Attribute.Type.DOUBLE).attribute("finalTemp", Attribute.Type.DOUBLE);

        final StreamDefinition fireStreamDefinition = StreamDefinition.id("RoomTemperatureAlertStream").attribute("roomNo", Attribute.Type.STRING)
                .attribute("initialTemp", Attribute.Type.DOUBLE).attribute("finalTemp", Attribute.Type.DOUBLE);

        Attribute.Type[] roomTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                roomStreamDefinition.getAttributeList());
        Attribute.Type[] fireTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                fireStreamDefinition.getAttributeList());

        siddhiAppRuntime.addCallback("RoomTemperatureAlertStream", new StreamCallback() {
            int eventCount = 0;

            @Override
            public void receive(Event[] events) {
                //System.out.println("Triggered fire event");
                for (Event event : events) {
                    eventCount++;
                    //System.out.println("Sink received fire event " + event);
                    ArrayList<Event> arrayList = new ArrayList<>(1);
                    arrayList.add(event);
                    try {
                        tcpNettyClient.send("RoomTemperatureAlertStream", BinaryEventConverter.convertToBinaryMessage(
                                arrayList.toArray(new Event[1]), fireTypes).array()).await();
                    } catch (InterruptedException | IOException e) {
                        LOG.error(e);
                    }
                }
            }
        });

        TCPNettyServer tcpNettyServer = new TCPNettyServer();

        tcpNettyServer.addStreamListener(new StreamListener() {

            @Override
            public String getChannelId() {
                return roomStreamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), roomTypes));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                //LOG.info(event);
                //eventQueue.add(new InputHandlerEventTuple(hsInputHandler, event));
                try {
                    rsInputHandler.send(event);
                } catch (InterruptedException e) {}
                /*synchronized (LOCK) {
                    LOCK.notify();
                }*/
            }
        });

        ServerConfig sc = new ServerConfig();
        sc.setPort(9892);
        tcpNettyServer.start(sc);

        /*for (int i = 0; i < numberThreads; i++) {
            new Thread(TcpServerSampleTwoStateRule::Run).start();
        }*/
    }

    public static void Run() {
        while (keepRunning) {
            try {
                synchronized (LOCK) {
                    LOCK.wait();
                }
            } catch (InterruptedException e) {

            }
            InputHandlerEventTuple tuple;
            tuple = eventQueue.remove(0);
            try {
                int j = 0;
                while (++j < 1000)
                    tuple.handler.send(tuple.event);
            } catch (InterruptedException e) {

            }
        }
    }
}
