package io.siddhi.sample.tcpsamples.tcpserver;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.InputHandler;
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

public class TcpServerSampleNoRuleNoSink {
    static final Logger LOG = Logger.getLogger(TcpServerSampleNoRule.class);
    private static boolean keepRunning = true;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("USAGE: java quick-start-samples.jar -classpath io.siddhi.sample.tcpsamples.tcpserver.TcpServerSampleNoRuleNoSink <sink_ip_address> <number_threads>");
            System.exit(0);
        }
        for (int i = 0; i < Integer.parseInt(args[1]); i++) {
            new Thread(() -> {
                SiddhiManager siddhiManager = new SiddhiManager();

                String inStreamDefinition = "" +
                        "define stream humidityStream (percentage int, area string);" +
                        "" +
                        "define stream temperatureStream (value int, area string);" +
                        "" +
                        "define stream smokeStream (density int, area string);" +
                        "" +
                        "define stream heatcameraStream (percChanceOfFire int, area string);";

                SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
                siddhiAppRuntime.start();

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


                Attribute.Type[] temperatureTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        temperatureStreamDefinition.getAttributeList());
                Attribute.Type[] humidityTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        humidityStreamDefinition.getAttributeList());
                Attribute.Type[] smokeTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        smokeStreamDefinition.getAttributeList());
                Attribute.Type[] heatcameraTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                        heatcameraStreamDefinition.getAttributeList());

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
                        /*LOG.info(event);*/
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
                        /*LOG.info(event);*/
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
                        /*LOG.info(event);*/
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
