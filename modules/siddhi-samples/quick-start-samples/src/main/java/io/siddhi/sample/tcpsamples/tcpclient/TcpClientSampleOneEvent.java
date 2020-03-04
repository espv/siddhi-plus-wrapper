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

public class TcpClientSampleOneEvent {
    static final Logger LOG = Logger.getLogger(TcpClientSampleOneEvent.class);
    private static boolean keepRunning = true;

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

        final StreamDefinition humidityStreamDefinition = StreamDefinition.id("humidityStream").attribute("percentage", Attribute.Type.INT)
                .attribute("area", Attribute.Type.STRING);

        final StreamDefinition temperatureStreamDefinition = StreamDefinition.id("temperatureStream").attribute("value", Attribute.Type.INT)
                .attribute("area", Attribute.Type.STRING);

        Attribute.Type[] temperatureTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                temperatureStreamDefinition.getAttributeList());
        Attribute.Type[] humidityTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                humidityStreamDefinition.getAttributeList());

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect(args[0], 9892);
        boolean sendHumidity = false;
        while (keepRunning) {
            ArrayList<Event> arrayList = new ArrayList<>(1);
            if (sendHumidity) {
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{1, "office"}));
                try {
                    tcpNettyClient.send("humidityStream", BinaryEventConverter.convertToBinaryMessage(
                            arrayList.toArray(new Event[1]), humidityTypes).array()).await();
                } catch (IOException e) {
                    LOG.error(e);
                }
            } else {
                // Sending humidity
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{433, "office"}));
                try {
                    tcpNettyClient.send("temperatureStream", BinaryEventConverter.convertToBinaryMessage(
                            arrayList.toArray(new Event[1]), temperatureTypes).array()).await();
                } catch (IOException e) {
                    LOG.error(e);
                }
            }
            //sendHumidity = !sendHumidity;
        }

        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();

        siddhiAppRuntime.shutdown();
    }
}
