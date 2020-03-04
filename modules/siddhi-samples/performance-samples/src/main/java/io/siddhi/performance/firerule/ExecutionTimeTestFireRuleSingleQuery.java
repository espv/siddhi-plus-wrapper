
package io.siddhi.performance.firerule;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;

public class ExecutionTimeTestFireRuleSingleQuery {
    private volatile static boolean keepRunning = true;
    static volatile long totalEventCount = 0;
    static volatile int threadCnt = 0;
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("USAGE: java performance-samples.jar -classpath io.siddhi.performance.firerule.FireRuleSingleQuery <number_threads>");
            System.exit(0);
        }
        System.out.println(System.getProperty("java.vm.name"));
        SiddhiManager siddhiManager1 = new SiddhiManager();
        String siddhiApp = "" +
                //" @App:Statistics(reporter = 'console', interval = '5') " +
                " define stream humidityStream (percentage int, area string, timestamp long); " +
                "" +
                " define stream temperatureStream (value int, area string, timestamp long); " +
                "" +
                " define stream temperatureStream2 (value int, area string, timestamp long); " +
                "" +
                " define stream fireStream1 (percentage int, area string, timestamp long); " +
                "" +
                " define stream randomStream (timestamp long); " +
                "" +
                " @info(name = 'query1') " +
                " from every (h = humidityStream [percentage < 25] -> t = temperatureStream [value > 45]) within 500 seconds " +
                " select h.percentage, h.area, h.timestamp " +
                " insert into fireStream1 ; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(siddhiApp);
        StreamCallback sc = new StreamCallback() {
            long eventCount = 0;
            long timeSpent = 0;
            long startTime = System.currentTimeMillis();

            @Override
            public void receive(Event[] events) {  // 18
                for (Event event : events) {
                    eventCount++;
                    if (eventCount % 100000 == 0) {
                        timeSpent = System.currentTimeMillis() - startTime;
                        totalEventCount += eventCount;
                        System.out.println("Received " + eventCount + " events in " + timeSpent + " milliseconds; total event count: " + totalEventCount);
                        //System.out.println("Throughput : " + (eventCount * 1000) / ((System.currentTimeMillis()) -
                        //        startTime));
                        //System.out.println("Time spent :  " + (timeSpent * 1.0 / eventCount));
                        startTime = System.currentTimeMillis();
                        eventCount = 0;
                        timeSpent = 0;
                    }
                }
            }
        };

        siddhiAppRuntime1.addCallback("fireStream1", sc);
        InputHandler hsInputHandler1 = siddhiAppRuntime1.getInputHandler("humidityStream");

        InputHandler[] hsInputHandlers = new InputHandler[]{hsInputHandler1};

        InputHandler tsInputHandler1 = siddhiAppRuntime1.getInputHandler("temperatureStream");

        siddhiAppRuntime1.start();

        try {
            for (int j = 0; j < 1000000; j++) {
                hsInputHandler1.send(new Object[]{26, "office", System.currentTimeMillis()});
                tsInputHandler1.send(new Object[]{33, "office", System.currentTimeMillis()});
            }
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
        while (keepRunning) {
            try {
                //for (int j = 0; j < 9; j++) {
                    hsInputHandler1.send(new Object[]{26, "office", System.currentTimeMillis()});
                    tsInputHandler1.send(new Object[]{33, "office", System.currentTimeMillis()});
                //}
                //long k = 0;
                //while (k++ < 1000000000L);
                //System.out.println("\n\n\n");
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received signal to shut down gracefully");
            keepRunning = false;
        }));
    }
}
