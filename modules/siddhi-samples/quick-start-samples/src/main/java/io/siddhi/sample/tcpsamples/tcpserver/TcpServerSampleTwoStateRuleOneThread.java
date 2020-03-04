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


public class TcpServerSampleTwoStateRuleOneThread {
    private static final Logger LOG = Logger.getLogger(TcpServerSampleTwoStateRule.class);
    private static boolean keepRunning = true;
    private static TCPNettyClient tcpNettyClient;
    private static volatile Semaphore s = new Semaphore(0);
    private static final List<InputHandlerEventTuple> eventQueue = Collections.synchronizedList(new ArrayList<>());

    public static String createQueries(int number) {
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < number; i++) {
            query.append("@info(name = 'query").append(i).append("') ").append("from every (h = humidityStream [percentage < 25] -> t = temperatureStream [value > 45]) within 5 seconds ").append("select value, percentage, h.area ").append("insert into fireStream ;");
        }

        return query.toString();
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("USAGE: java quick-start-samples.jar -classpath io.siddhi.sample.tcpsamples.tcpserver.TcpServerSample <sink_ip_address> <number_threads> <number_events>");
            System.exit(0);
        }

        String ip = args[0];

        int numberThreads = Integer.parseInt(args[1]);
        int numberEvents = Integer.parseInt(args[2]);
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
                "@async(buffer.size='10000', workers='2', batch.size.max='10000')" +
                "define stream humidityStream (percentage int, area string);" +
                "" +
                "@async(buffer.size='10000', workers='2', batch.size.max='10000')" +
                "define stream temperatureStream (value int, area string);" +
                "" +
                "define stream fireStream (value int, percentage int, area string);";
        String query = createQueries(100);
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
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), humidityTypes));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                try {
                    //int j = 0;
                    //while (++j < 1000)
                    hsInputHandler.send(event);
                } catch (InterruptedException e) {

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
                int j = 0;
                while (j++ < numberEvents)
                    onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), temperatureTypes));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                try {
                    tsInputHandler.send(event);
                } catch (InterruptedException e) {

                }
            }

        });

        ServerConfig sc = new ServerConfig();
        sc.setPort(9892);
        tcpNettyServer.start(sc);
    }
}
