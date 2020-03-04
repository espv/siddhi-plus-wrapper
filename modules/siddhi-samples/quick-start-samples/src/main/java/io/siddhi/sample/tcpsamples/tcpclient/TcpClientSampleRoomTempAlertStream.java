package io.siddhi.sample.tcpsamples.tcpclient;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;

import java.io.IOException;
import java.util.ArrayList;

public class TcpClientSampleRoomTempAlertStream {
    private static final Logger LOG = Logger.getLogger(TcpClientSampleRoomTempAlertStream.class);
    private static boolean keepRunning = true;
    static int delay = 500000;
    static int delay_cnt = 0;

    public static void main(String[] args) throws InterruptedException, ConnectionUnavailableException {
        if (args.length < 1) {
            System.out.println("USAGE: java quick-start-samples.jar -classpath io.siddhi.sample.tcpsamples.tcpclient.TcpClientSample <server_ip_address>");
            System.exit(0);
        }
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream humidityStream (percentage int, area string);" +
                "" +
                "define stream temperatureStream (value int, area string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);

        final StreamDefinition roomStreamDefinition = StreamDefinition.id("RoomTemperatureAlertStream").attribute("roomNo", Attribute.Type.STRING)
                .attribute("initialTemp", Attribute.Type.DOUBLE).attribute("finalTemp", Attribute.Type.DOUBLE);

        Attribute.Type[] roomTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                roomStreamDefinition.getAttributeList());

        TCPNettyClient tcpNettyClient = new TCPNettyClient(true, true);
        tcpNettyClient.connect(args[0], 9892);
        while (keepRunning) {
            ArrayList<Event> arrayList = new ArrayList<>(1);

            arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"2", 43.34, 53.62}));
            try {
                tcpNettyClient.send("RoomTemperatureStream", BinaryEventConverter.convertToBinaryMessage(
                        arrayList.toArray(new Event[1]), roomTypes).array()).await();
            } catch (IOException e) {
                LOG.error(e);
            }
        }

        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();

        siddhiAppRuntime.shutdown();
    }
}
