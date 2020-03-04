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

public class TcpClientSampleTwoEvents {
    private static final Logger LOG = Logger.getLogger(TcpClientSampleTwoEvents.class);
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

        final StreamDefinition humidityStreamDefinition = StreamDefinition.id("humidityStream").attribute("percentage", Attribute.Type.INT)
                .attribute("area", Attribute.Type.STRING);

        final StreamDefinition temperatureStreamDefinition = StreamDefinition.id("temperatureStream").attribute("value", Attribute.Type.INT)
                .attribute("area", Attribute.Type.STRING);

        Attribute.Type[] temperatureTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                temperatureStreamDefinition.getAttributeList());
        Attribute.Type[] humidityTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                humidityStreamDefinition.getAttributeList());

        TCPNettyClient tcpNettyClient = new TCPNettyClient(true, true);
        tcpNettyClient.connect(args[0], 9892);
        /*for (int i = 0; i < 1000000; i++) {
            ArrayList<Event> arrayList = new ArrayList<>(1);
            if (sendTemperature) {
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
            sendTemperature = !sendTemperature;
        }*/
        long start = 0;
        long total = 0;
        boolean sendTemperature = false;
        while (keepRunning) {
            /*try {
                byte[] a;
                byte[] b;
                ArrayList<Event> arrayList = new ArrayList<>(1);
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{1, "office"}));
                a = BinaryEventConverter.convertToBinaryMessage(
                        arrayList.toArray(new Event[1]), humidityTypes).array();

                ArrayList<Event> arrayList2 = new ArrayList<>(1);
                arrayList2.add(new Event(System.currentTimeMillis(), new Object[]{433, "office"}));
                b = BinaryEventConverter.convertToBinaryMessage(
                        arrayList2.toArray(new Event[1]), temperatureTypes).array();

                byte[] c = new byte[1200];
                for (int i = 0; i < 1000; i++) {
                    if ((a.length+1)*i > 1200)
                        break;
                    System.arraycopy(a, 0, c, a.length*i, a.length);
                }

                byte[] d = new byte[1200];
                for (int i = 0; i < 1000; i++) {
                    if ((b.length+1)*i > 1200)
                        break;
                    System.arraycopy(b, 0, d, b.length*i, b.length);
                }

                tcpNettyClient.send("humidityStream", c).await();

                tcpNettyClient.send("temperatureStream", d).await();
            } catch (IOException e) {
                System.exit(0);
            }

            continue;*/

            int iterations = 200000;
            for (int i = 0; i < iterations; i++) {
                ArrayList<Event> arrayList = new ArrayList<>(1);
                if (sendTemperature) {
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
                sendTemperature = !sendTemperature;
            }
            if (start == 0) {
                start = System.nanoTime();
            }
            //LOG.info(event);
            total += iterations;
            System.out.println("Client transmitted event " + total + ", " + iterations + " events in " + (System.nanoTime()-start));
            start = System.nanoTime();
            //long start = System.nanoTime();
            ArrayList<Event> arrayList = new ArrayList<>(1);
            if (!sendTemperature) {
                // Sending humidity
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{1, "office"}));
                try {
                    tcpNettyClient.send("humidityStream", BinaryEventConverter.convertToBinaryMessage(
                            arrayList.toArray(new Event[1]), humidityTypes).array()).await();
                } catch (IOException e) {
                    LOG.error(e);
                }
            } else {
                // Sending temperature
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{433, "office"}));
                try {
                    tcpNettyClient.send("temperatureStream", BinaryEventConverter.convertToBinaryMessage(
                            arrayList.toArray(new Event[1]), temperatureTypes).array()).await();
                } catch (IOException e) {
                    LOG.error(e);
                }
            }
            sendTemperature = !sendTemperature;
            /*long start = System.nanoTime();
            while (System.nanoTime()-start < delay);
            if (++delay_cnt > 1) {
                delay_cnt = 0;
                delay -= 10;
            }*/
            //while (System.nanoTime()-start < 57500);  // No sink, two state rule, limit
            //while (System.nanoTime()-start < 50000);
           Thread.sleep(1000);
           //Thread.sleep(5000);
        }

        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();

        siddhiAppRuntime.shutdown();
    }
}
