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

public class TcpSinkSampleEventTemp {
    static final Logger LOG = Logger.getLogger(TcpSinkSampleEventTemp.class);
    private static boolean keepRunning = true;

    public static void main(String[] args) {
        final StreamDefinition tempStreamDefinition = StreamDefinition.id("temperatureStream").attribute("value", Attribute.Type.INT)
                .attribute("area", Attribute.Type.STRING);

        Attribute.Type[] tempTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                tempStreamDefinition.getAttributeList());

        TCPNettyServer tcpNettyServer = new TCPNettyServer();
        ServerConfig sc = new ServerConfig();
        sc.setPort(9893);
        tcpNettyServer.start(sc);

        tcpNettyServer.addStreamListener(new StreamListener() {
            int number_rcvd = 0;

            @Override
            public String getChannelId() {
                return tempStreamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), tempTypes));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                if (++number_rcvd % 100000 == 0) {
                    //LOG.info(event);
                    System.out.println("Sink received temperature event " + event + " number " + ++number_rcvd);
                }
            }
        });
    }
}
