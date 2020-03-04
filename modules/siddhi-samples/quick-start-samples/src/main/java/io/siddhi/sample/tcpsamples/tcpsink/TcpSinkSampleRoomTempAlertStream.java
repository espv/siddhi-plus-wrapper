package io.siddhi.sample.tcpsamples.tcpsink;

import io.siddhi.core.event.Event;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyServer;
import org.wso2.extension.siddhi.io.tcp.transport.callback.StreamListener;
import org.wso2.extension.siddhi.io.tcp.transport.config.ServerConfig;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;

import java.nio.ByteBuffer;

public class TcpSinkSampleRoomTempAlertStream {
    static final Logger LOG = Logger.getLogger(TcpSinkSampleRoomTempAlertStream.class);
    private static boolean keepRunning = true;

    public static void main(String[] args) {
        final StreamDefinition fireStreamDefinition = StreamDefinition.id("RoomTemperatureAlertStream").attribute("roomNo", Attribute.Type.STRING)
                .attribute("initialTemp", Attribute.Type.DOUBLE).attribute("finalTemp", Attribute.Type.DOUBLE);

        Attribute.Type[] fireTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                fireStreamDefinition.getAttributeList());

        TCPNettyServer tcpNettyServer = new TCPNettyServer();
        ServerConfig sc = new ServerConfig();
        sc.setPort(9893);
        tcpNettyServer.start(sc);

        tcpNettyServer.addStreamListener(new StreamListener() {
            int number_rcvd = 0;

            @Override
            public String getChannelId() {
                return fireStreamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), fireTypes));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                if (++number_rcvd % 100000 == 0) {
                    LOG.info(event);
                    System.out.println("Sink received fire event " + event + " number " + number_rcvd);
                }
            }

        });

        while (keepRunning);

        tcpNettyServer.shutdownGracefully();
    }
}
