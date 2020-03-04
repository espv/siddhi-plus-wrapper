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

public class TcpServerSampleFourStateRuleNoSink {
    static final Logger LOG = Logger.getLogger(TcpServerSampleFourStateRuleNoSink.class);
    private static boolean keepRunning = true;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("USAGE: java quick-start-samples.jar -classpath io.siddhi.sample.tcpsamples.tcpserver.TcpServerSample <sink_ip_address> <number_threads>");
            System.exit(0);
        }
        for (int i = 0; i < Integer.parseInt(args[1]); i++) {
            new Thread(() -> {
                SiddhiManager siddhiManager = new SiddhiManager();

                String inStreamDefinition = "" +
                        " define stream humidityStream (percentage int, area string);" +
                        "" +
                        " define stream temperatureStream (value int, area string);" +
                        "" +
                        " define stream smokeStream (density int, area string);" +
                        "" +
                        " define stream heatcameraStream (percChanceOfFire int, area string);" +
                        "" +
                        " define stream fireStream (value int, percentage int, density int, percChanceOfFire int, area string);";
                String query = "" +
                        " @info(name = 'query1') " +
                        " from every (h = humidityStream [percentage < 25] -> t = temperatureStream [value > 45] -> s = smokeStream [density > 50] -> c = heatcameraStream [percChanceOfFire > 65]) within 5 seconds " +
                        " select value, percentage, density, percChanceOfFire, h.area " +
                        " insert into fireStream ;";
                SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
                siddhiAppRuntime.start();
                InputHandler hsInputHandler = siddhiAppRuntime.getInputHandler("humidityStream");
                InputHandler tsInputHandler = siddhiAppRuntime.getInputHandler("temperatureStream");
                InputHandler ssInputHandler = siddhiAppRuntime.getInputHandler("smokeStream");
                InputHandler csInputHandler = siddhiAppRuntime.getInputHandler("heatcameraStream");

                TCPNettyClient tcpNettyClient = new TCPNettyClient();
                try {
                    tcpNettyClient.connect(args[0], 9893);
                } catch (ConnectionUnavailableException e) {
                    e.printStackTrace();
                }

                final StreamDefinition humidityStreamDefinition = StreamDefinition.id("humidityStream").attribute("percentage", Attribute.Type.INT)
                        .attribute("area", Attribute.Type.STRING);

                final StreamDefinition temperatureStreamDefinition = StreamDefinition.id("temperatureStream").attribute("value", Attribute.Type.INT)
                        .attribute("area", Attribute.Type.STRING);

                final StreamDefinition smokeStreamDefinition = StreamDefinition.id("smokeStream").attribute("density", Attribute.Type.INT)
                        .attribute("area", Attribute.Type.STRING);

                final StreamDefinition heatcameraStreamDefinition = StreamDefinition.id("heatcameraStream").attribute("percChanceOfFire", Attribute.Type.INT)
                        .attribute("area", Attribute.Type.STRING);

                final StreamDefinition fireStreamDefinition = (
                        StreamDefinition.id("fireStream")
                                        .attribute("value", Attribute.Type.INT)
                                        .attribute("percentage", Attribute.Type.INT)
                                        .attribute("density", Attribute.Type.INT)
                                        .attribute("percChanceOfFire", Attribute.Type.INT)
                                        .attribute("area", Attribute.Type.STRING));

                Attribute.Type[] temperatureTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        temperatureStreamDefinition.getAttributeList());
                Attribute.Type[] humidityTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        humidityStreamDefinition.getAttributeList());
                Attribute.Type[] smokeTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        smokeStreamDefinition.getAttributeList());
                Attribute.Type[] heatcameraTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        smokeStreamDefinition.getAttributeList());
                Attribute.Type[] fireTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        fireStreamDefinition.getAttributeList());

                siddhiAppRuntime.addCallback("fireStream", new StreamCallback() {
                    int eventCount = 0;

                    @Override
                    public void receive(Event[] events) {
                        //System.out.println("Triggered fire event");
                        for (Event event : events) {
                            eventCount++;
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
                        //LOG.info(event);
                        try {
                            hsInputHandler.send(event);
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
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
                        onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), temperatureTypes));
                    }

                    public void onEvents(Event[] events) {
                        for (Event event : events) {
                            onEvent(event);
                        }
                    }

                    public void onEvent(Event event) {
                        //LOG.info(event);
                        try {
                            tsInputHandler.send(event);
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    }

                });

                tcpNettyServer.addStreamListener(new StreamListener() {

                    @Override
                    public String getChannelId() {
                        return smokeStreamDefinition.getId();
                    }

                    @Override
                    public void onMessage(byte[] message) {
                        onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), smokeTypes));
                    }

                    public void onEvents(Event[] events) {
                        for (Event event : events) {
                            onEvent(event);
                        }
                    }

                    public void onEvent(Event event) {
                        //LOG.info(event);
                        try {
                            ssInputHandler.send(event);
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    }

                });

                tcpNettyServer.addStreamListener(new StreamListener() {

                    @Override
                    public String getChannelId() {
                        return heatcameraStreamDefinition.getId();
                    }

                    @Override
                    public void onMessage(byte[] message) {
                        onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), heatcameraTypes));
                    }

                    public void onEvents(Event[] events) {
                        for (Event event : events) {
                            onEvent(event);
                        }
                    }

                    public void onEvent(Event event) {
                        //LOG.info(event);
                        try {
                            csInputHandler.send(event);
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    }

                });

                ServerConfig sc = new ServerConfig();
                sc.setPort(9892);
                tcpNettyServer.start(sc);

                while (keepRunning);

                siddhiAppRuntime.shutdown();

                tcpNettyServer.shutdownGracefully();
            }).start();
        }

        while (keepRunning);
    }
}
