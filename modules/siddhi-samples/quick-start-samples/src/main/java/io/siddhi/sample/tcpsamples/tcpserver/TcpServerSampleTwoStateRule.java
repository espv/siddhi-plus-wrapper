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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

class InputHandlerEventTuple {
    public final InputHandler handler;
    public final Event event;

    public InputHandlerEventTuple(InputHandler handler, Event event) {
        this.handler = handler;
        this.event = event;
    }
}

public class TcpServerSampleTwoStateRule {
    private static final Logger LOG = Logger.getLogger(TcpServerSampleTwoStateRule.class);
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
        tcpNettyClient = new TCPNettyClient();
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
                "define stream humidityStream (percentage int, area string);" +
                "" +
                "define stream temperatureStream (value int, area string);" +
                "" +
                "define stream fireStream (value int, percentage int, area string);";
        String query = "" +
                "@info(name = 'query1') " +
                "from every (h = humidityStream [percentage < 25] -> t = temperatureStream [value > 45]) within 5 seconds " +
                "select value, percentage, h.area " +
                "insert into fireStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.start();
        InputHandler hsInputHandler = siddhiAppRuntime.getInputHandler("humidityStream");
        InputHandler tsInputHandler = siddhiAppRuntime.getInputHandler("temperatureStream");

        final StreamDefinition humidityStreamDefinition = StreamDefinition.id("humidityStream").attribute("percentage", Attribute.Type.INT)
                .attribute("area", Attribute.Type.STRING);

        final StreamDefinition temperatureStreamDefinition = StreamDefinition.id("temperatureStream").attribute("value", Attribute.Type.INT)
                .attribute("area", Attribute.Type.STRING);

        final StreamDefinition fireStreamDefinition = StreamDefinition.id("fireStream").attribute("value", Attribute.Type.INT)
                .attribute("percentage", Attribute.Type.INT).attribute("area", Attribute.Type.STRING);

        Attribute.Type[] temperatureTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                temperatureStreamDefinition.getAttributeList());
        Attribute.Type[] humidityTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                humidityStreamDefinition.getAttributeList());
        Attribute.Type[] fireTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                fireStreamDefinition.getAttributeList());

        siddhiAppRuntime.addCallback("fireStream", new StreamCallback() {
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
                        Instant time = Instant.now();
                        System.out.println("s: " + time);
                        tcpNettyClient.send("fireStream", BinaryEventConverter.convertToBinaryMessage(
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
                return humidityStreamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                //int time = Instant.now().getNano();
                //int overhead = Instant.now().getNano();
                //Instant time = time - (Instant.now());
                Instant time = Instant.now();
                System.out.println("r: " + time);
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), humidityTypes));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                //LOG.info(event);
                eventQueue.add(new InputHandlerEventTuple(hsInputHandler, event));
                synchronized (LOCK) {
                    LOCK.notify();
                }
            }
        });

        tcpNettyServer.addStreamListener(new StreamListener() {

            @Override
            public String getChannelId() {
                return temperatureStreamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                //int time = Instant.now().getNano();
                //int overhead = Instant.now().getNano();
                //Instant time = time - (Instant.now());
                Instant time = Instant.now();
                System.out.println("r: " + time);
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), temperatureTypes));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                eventQueue.add(new InputHandlerEventTuple(tsInputHandler, event));
                synchronized (LOCK) {
                    LOCK.notify();
                }
            }
        });

        ServerConfig sc = new ServerConfig();
        sc.setPort(9892);
        tcpNettyServer.start(sc);

        for (int i = 0; i < numberThreads; i++) {
            new Thread(TcpServerSampleTwoStateRule::Run).start();
        }
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
