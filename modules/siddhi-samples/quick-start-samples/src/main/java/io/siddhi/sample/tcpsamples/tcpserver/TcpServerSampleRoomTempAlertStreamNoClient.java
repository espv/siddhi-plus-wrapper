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

public class TcpServerSampleRoomTempAlertStreamNoClient {
    private static final Logger LOG = Logger.getLogger(TcpServerSampleRoomTempAlertStreamNoClient.class);
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

        tcpNettyClient = new TCPNettyClient(true, true);
        try {
            tcpNettyClient.connect(ip, 9893);
        } catch (ConnectionUnavailableException e) {
            e.printStackTrace();
        }

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
        InputHandler rsInputHandler1 = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");
        InputHandler rsInputHandler2 = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");
        InputHandler rsInputHandler3 = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");
        InputHandler rsInputHandler4 = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");
        InputHandler rsInputHandler5 = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");
        InputHandler rsInputHandler6 = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");
        InputHandler rsInputHandler7 = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");
        InputHandler rsInputHandler8 = siddhiAppRuntime.getInputHandler("RoomTemperatureAlertStream");
        InputHandler[] rsInputHandlers = new InputHandler[]{rsInputHandler1,rsInputHandler2,rsInputHandler3,rsInputHandler4,
                                                            rsInputHandler5,rsInputHandler6,rsInputHandler7,rsInputHandler8};

        final StreamDefinition fireStreamDefinition = StreamDefinition.id("RoomTemperatureAlertStream").attribute("roomNo", Attribute.Type.STRING)
                .attribute("initialTemp", Attribute.Type.DOUBLE).attribute("finalTemp", Attribute.Type.DOUBLE);

        Attribute.Type[] roomAlertTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                fireStreamDefinition.getAttributeList());

        siddhiAppRuntime.addCallback("RoomTemperatureAlertStream", new StreamCallback() {
            int eventCount = 0;

            @Override
            public void receive(Event[] events) {
                //System.out.println("Triggered fire event");
                ArrayList<Event> arrayList = new ArrayList<>(events.length);
                Collections.addAll(arrayList, events);
                try {
                    tcpNettyClient.send("RoomTemperatureAlertStream", BinaryEventConverter.convertToBinaryMessage(
                            arrayList.toArray(new Event[events.length]), roomAlertTypes).array()).await();
                } catch (InterruptedException | IOException e) {
                    LOG.error(e);
                }
            }
        });

        Event event = new Event(System.currentTimeMillis(), new Object[]{"2", 43.34, 53.62});
        for (int i = 0; i < 8; i++) {
            final int cnt = i;
            new Thread(() -> {
                final int tid = cnt;
                while (keepRunning) {
                    try {
                        rsInputHandlers[0].send(event);
                    } catch (InterruptedException e) {

                    }
                }
            }).start();
        }
        /*while (keepRunning) {
            try {
                rsInputHandler.send(event);
            } catch (InterruptedException e) {

            }
        }*/
    }
}
