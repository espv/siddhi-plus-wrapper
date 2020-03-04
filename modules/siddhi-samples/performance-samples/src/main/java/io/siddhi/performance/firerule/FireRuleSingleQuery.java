
package io.siddhi.performance.firerule;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;

import java.util.concurrent.CountDownLatch;

public class FireRuleSingleQuery {
    private volatile static boolean keepRunning = true;
    static volatile long totalEventCount = 0;
    static volatile int threadCnt = 0;
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("USAGE: java performance-samples.jar -classpath io.siddhi.performance.firerule.FireRuleSingleQuery <number_threads>");
            System.exit(0);
        }
       /* boolean cont = true;
        while (cont) {
            long start = System.nanoTime();
            int[] arr = new int[1000000000];
            for (int i = 0; i < 1000000000; i++) {
                arr[i] = 1000;
            }
            long stop = System.nanoTime();
            System.out.println(stop-start);
        }*/
        System.out.println(System.getProperty("java.vm.name"));
        SiddhiManager siddhiManager1 = new SiddhiManager();
        /*SiddhiManager siddhiManager2 = new SiddhiManager();
        SiddhiManager siddhiManager3 = new SiddhiManager();
        SiddhiManager siddhiManager4 = new SiddhiManager();
        SiddhiManager siddhiManager5 = new SiddhiManager();
        SiddhiManager siddhiManager6 = new SiddhiManager();
        SiddhiManager siddhiManager7 = new SiddhiManager();
        SiddhiManager siddhiManager8 = new SiddhiManager();*/

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
        /*SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(siddhiApp);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(siddhiApp);
        SiddhiAppRuntime siddhiAppRuntime4 = siddhiManager4.createSiddhiAppRuntime(siddhiApp);
        SiddhiAppRuntime siddhiAppRuntime5 = siddhiManager5.createSiddhiAppRuntime(siddhiApp);
        SiddhiAppRuntime siddhiAppRuntime6 = siddhiManager6.createSiddhiAppRuntime(siddhiApp);
        SiddhiAppRuntime siddhiAppRuntime7 = siddhiManager7.createSiddhiAppRuntime(siddhiApp);
        SiddhiAppRuntime siddhiAppRuntime8 = siddhiManager8.createSiddhiAppRuntime(siddhiApp);*/

        SiddhiAppRuntime[] siddhiAppRuntimes = new SiddhiAppRuntime[]{siddhiAppRuntime1/*,siddhiAppRuntime2,siddhiAppRuntime3,
                siddhiAppRuntime4,siddhiAppRuntime5,siddhiAppRuntime6,siddhiAppRuntime7,siddhiAppRuntime8*/};
        StreamCallback sc = new StreamCallback() {
            long eventCount = 0;
            long timeSpent = 0;
            long startTime = System.currentTimeMillis();

            @Override
            public void receive(Event[] events) {  // 18
                //System.out.println("Triggered fire event");
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

        for (SiddhiAppRuntime sar : siddhiAppRuntimes) {
            sar.addCallback("fireStream1", sc);
            /*sar.addCallback("fireStream2", sc);
            sar.addCallback("fireStream3", sc);
            sar.addCallback("fireStream4", sc);
            sar.addCallback("fireStream5", sc);
            sar.addCallback("fireStream6", sc);
            sar.addCallback("fireStream7", sc);
            sar.addCallback("fireStream8", sc);
            sar.addCallback("fireStream9", sc);
            sar.addCallback("fireStream10", sc);*/
        }

        InputHandler hsInputHandler1 = siddhiAppRuntime1.getInputHandler("humidityStream");
        /*InputHandler hsInputHandler2 = siddhiAppRuntime2.getInputHandler("humidityStream");
        InputHandler hsInputHandler3 = siddhiAppRuntime3.getInputHandler("humidityStream");
        InputHandler hsInputHandler4 = siddhiAppRuntime4.getInputHandler("humidityStream");
        InputHandler hsInputHandler5 = siddhiAppRuntime5.getInputHandler("humidityStream");
        InputHandler hsInputHandler6 = siddhiAppRuntime6.getInputHandler("humidityStream");
        InputHandler hsInputHandler7 = siddhiAppRuntime7.getInputHandler("humidityStream");
        InputHandler hsInputHandler8 = siddhiAppRuntime8.getInputHandler("humidityStream");*/

        InputHandler[] hsInputHandlers = new InputHandler[]{hsInputHandler1/*,hsInputHandler2,hsInputHandler3,
                hsInputHandler4,hsInputHandler5,hsInputHandler6,hsInputHandler7,hsInputHandler8*/};

        InputHandler tsInputHandler1 = siddhiAppRuntime1.getInputHandler("temperatureStream");
        /*InputHandler tsInputHandler2 = siddhiAppRuntime2.getInputHandler("temperatureStream");
        InputHandler tsInputHandler3 = siddhiAppRuntime3.getInputHandler("temperatureStream");
        InputHandler tsInputHandler4 = siddhiAppRuntime4.getInputHandler("temperatureStream");
        InputHandler tsInputHandler5 = siddhiAppRuntime5.getInputHandler("temperatureStream");
        InputHandler tsInputHandler6 = siddhiAppRuntime6.getInputHandler("temperatureStream");
        InputHandler tsInputHandler7 = siddhiAppRuntime7.getInputHandler("temperatureStream");
        InputHandler tsInputHandler8 = siddhiAppRuntime8.getInputHandler("temperatureStream");*/

        InputHandler[] tsInputHandlers = new InputHandler[]{tsInputHandler1/*,tsInputHandler2,tsInputHandler3,
                tsInputHandler4,tsInputHandler5,tsInputHandler6, tsInputHandler7,tsInputHandler8*/};
        //InputHandler rInputHandler = siddhiAppRuntime.getInputHandler("randomStream");
        siddhiAppRuntime1.start();
        /*siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();
        siddhiAppRuntime4.start();
        siddhiAppRuntime5.start();
        siddhiAppRuntime6.start();
        siddhiAppRuntime7.start();
        siddhiAppRuntime8.start();*/

        //for (int i = 0; i < Integer.parseInt(args[0]); i++) {
            //new Thread(() -> {
                int id = threadCnt++;
                int cnt = 0;
                try {
                    for (int j = 0; j < 1000000; j++) {
                        hsInputHandlers[0].send(new Object[]{26, "office", System.currentTimeMillis()});
                        //}
                        //else {
                        //if (j == 8) break;
                        tsInputHandlers[0].send(new Object[]{33, "office", System.currentTimeMillis()});
                        //Thread.sleep(5000);
                    }
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
                //System.exit(0);
                while (keepRunning) {
                    try {
                        //if (id % 2 == 0) {
                        //long start = System.nanoTime();
                        for (int j = 0; j < 9; j++) {
                            //hsInputHandlers[0].send(new Object[]{26, "office", System.currentTimeMillis()});
                            //}
                            //rInputHandler.send(new Object[]{System.currentTimeMillis()});
                            //else {
                            //Thread.sleep(5000);
                            tsInputHandlers[0].send(new Object[]{33, "office", System.currentTimeMillis()});
                            //Thread.sleep(1);
                        }
                        long k = 0;
                        while (k++ < 1000000000L);
                        System.out.println("\n\n\n");
                        //long stop = System.nanoTime();
                        //System.out.println("#" + (++cnt) + ": " + (stop-start));
                        //if (cnt % 1000000 == 0)
                        //    Thread.sleep(10000);
                        //}
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            //}).start();
        //}
        /*while (keepRunning) {
            hsInputHandler.send(new Object[]{22, "office", System.currentTimeMillis()});
            tsInputHandler.send(new Object[]{46, "office", System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
            rInputHandler.send(new Object[]{System.currentTimeMillis()});
        }*/

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received signal to shut down gracefully");
            keepRunning = false;
        }));
    }
}
