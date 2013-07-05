package me.schiz.jmeter.argentum.reporters;

import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.engine.util.NoThreadClone;
import org.apache.jmeter.reporters.AbstractListenerElement;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleListener;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class ArgentumListener extends AbstractListenerElement
        implements SampleListener, NoThreadClone, TestListener {

    private static final Logger log  = LoggingManager.getLoggerForClass();

    protected volatile int max_th = 1000;

    public final static int floatingSeconds = 2;
    public static String outputFileName = "ArgentumListener.OutputFileName";
    public static String timeout = "ArgentumListener.timeout";
    public static String percentiles = "ArgentumListener.percentiles";
    public static String timePeriods = "ArgentumListener.timePeriods";

    private volatile boolean started = false;
    protected BufferedWriter writer;

    protected ConcurrentSkipListSet<Long> secSet;
    protected ConcurrentHashMap<Long, Integer> threadsMap; //for active_threads metric
    protected ConcurrentHashMap<Long, AtomicInteger> throughputMap;  //for throughput metric
    protected ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicInteger>> responseCodeMap;   //ReturnCode -> Count per second map
    protected ConcurrentHashMap<Long, AtomicLong> sumLTMap;  //for average latency metric
    protected ConcurrentHashMap<Long, AtomicLong> sumInboundTraffic; // for avg inbound traffic metric
    protected ConcurrentHashMap<Long, AtomicLong> sumOutboundTraffic; // for avg outbound traffic metric

    protected ConcurrentHashMap<Long, AtomicLongArray> percentileDistMap; //seconds map for cumulative percentile distribution
    protected long[]    percentileDistShiftArray; //shift-array for cumulative percentile distribution
    protected ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicLongArray>>   samplerPercentileDistMap;
    protected ConcurrentHashMap<String, long[]>    samplerCumulativePercentileShiftArray;

    protected ConcurrentHashMap<String, AtomicLong> samplerTotalCounterMap;

//    protected volatile boolean isCalcQuantileDist = false;
//    protected volatile boolean isCalcIntervalDist = false;

    static String RC_OK = "200";
    static String RC_ERROR = "500";

    protected static ExecutorService executors;

    public void setOutputFileName(String filename) {
        setProperty(outputFileName, filename);
    }
    public String getOutputFileName() {
        return getPropertyAsString(outputFileName);
    }
    public void setPercentiles(String percs) {
        setProperty(percentiles, percs);
    }
    public float[] getPercentiles() {
        String x = getPropertyAsString(percentiles);
        String[] times = x.split(" ");
        float[] percs = new float[times.length];
        try{
        int j = 0;
            for(String time: times ) {
                percs[j++] = Float.parseFloat(time);
            }
        } catch (NumberFormatException nfe) {
            return ArgentumSecondRunnable.DEFAULT_QUANTILES;
        }
        return percs;
    }
    public void setTimePeriods(String tp) {
        setProperty(timePeriods, tp);
    }
    public int[] getTimePeriods() {
        String x = getPropertyAsString(timePeriods);
        String[] times = x.split(" ");
        int[] time_periods = new int[times.length];
        int j = 0;
        try{
            for(String time: times ) {
                time_periods[j++] = Integer.parseInt(time);
            }
        } catch (NumberFormatException nfe) {
            return ArgentumSecondRunnable.DEFAULT_TIME_PERIODS;
        }
        return time_periods;
    }
    public void setTimeout(int tmt) {
        setProperty(timeout, tmt);
    }
    public int getTimeout() {
        return getPropertyAsInt(timeout);
    }

    private boolean createSecond(Long second) {
        synchronized (this) {
            if(secSet.contains(second))    return true; //double check
            if(secSet.size() > 0) {
                if(secSet.first() > second)    return false; // very old SampleResult. Sorry
            }
            threadsMap.put(second, JMeterContextService.getNumberOfThreads());
            throughputMap.put(second, new AtomicInteger(0));
            sumLTMap.put(second, new AtomicLong(0));
            responseCodeMap.put(second, new ConcurrentHashMap<String, AtomicInteger>());
            //if(isCalcQuantileDist) {
            percentileDistMap.put(second, new AtomicLongArray(getTimeout() * 1000 + 1));
            samplerPercentileDistMap.put(second, new ConcurrentHashMap<String, AtomicLongArray>());
            //}
            sumInboundTraffic.put(second, new AtomicLong(0));
            sumOutboundTraffic.put(second, new AtomicLong(0));

            secSet.add(second);
        }

        if(secSet.size() > getTimeout()) {
            Long rSecond = secSet.pollFirst();

            int th = throughputMap.get(rSecond).get();
            if(th > max_th) max_th = th;

            if(executors != null) {
                log.info("execute new second");
                executors.execute(new ArgentumSecondRunnable(rSecond, //second
                        threadsMap.get(rSecond), //active_threads
                        throughputMap.get(rSecond).get(), //throughput
                        responseCodeMap.get(rSecond),
                        sumLTMap.get(rSecond).get(),
                        sumInboundTraffic.get(rSecond).get(),
                        sumOutboundTraffic.get(rSecond).get(),
                        true, //case distributions
                        percentileDistMap.get(rSecond),
                        percentileDistShiftArray,
                        samplerPercentileDistMap.get(rSecond),
                        samplerCumulativePercentileShiftArray,
                        samplerTotalCounterMap,
                        writer,
                        getTimeout()
                ));
            } else log.warn("Not found executors");

            threadsMap.remove(rSecond);
            throughputMap.remove(rSecond);
            sumLTMap.remove(rSecond);
            responseCodeMap.remove(rSecond);
            sumInboundTraffic.remove(rSecond);
            sumOutboundTraffic.remove(rSecond);
            percentileDistMap.remove(rSecond);
            samplerPercentileDistMap.remove(rSecond);
        }
        return true;
    }

    private void addRCtoMap(Long second, String rc) {
        ConcurrentHashMap<String, AtomicInteger> cursor = responseCodeMap.get(second);
        if(cursor.get(rc) == null) {
            synchronized (responseCodeMap) {
                if(cursor.get(rc) == null)  cursor.put(rc, new AtomicInteger(1));
                else cursor.get(rc).getAndIncrement();
            }
        } else cursor.get(rc).incrementAndGet();
    }

    private void addToSamplerDistMap(Long second, String title, int rt) {
        ConcurrentHashMap<String, AtomicLongArray> cursor = samplerPercentileDistMap.get(second);
        if(cursor.get(title) == null) {
            synchronized (samplerPercentileDistMap) {
                if(cursor.get(title) == null)  cursor.put(title, new AtomicLongArray(getTimeout() * 1000 + 1));
                else {
                    cursor.get(title).incrementAndGet(rt);
                }
            }
        } else {
            cursor.get(title).incrementAndGet(rt);
        }
    }

    @Override
    public void sampleOccurred(SampleEvent sampleEvent) {
        if(!started) return;
        SampleResult sr = sampleEvent.getResult();

        String samplerName = sr.getSampleLabel();
        long start = sr.getStartTime() / 1000;
        int rt = (int)sr.getTime();

        if(!secSet.contains(start)) {
            if(!createSecond(start)) {
                log.error("aggregation timeout");
                return;
            } else {
                log.info("second created");
            }
        }
        throughputMap.get(start).incrementAndGet();
        sumLTMap.get(start).getAndAdd(sampleEvent.getResult().getLatency());
        addRCtoMap(start, convertResponseCode(sr));
        sumInboundTraffic.get(start).getAndAdd(sampleEvent.getResult().getBodySize());
        sumOutboundTraffic.get(start).getAndAdd(sampleEvent.getResult().getHeadersSize());

        percentileDistMap.get(start).getAndIncrement(rt);
        addToSamplerDistMap(start, samplerName, rt);
    }

    @Override
    public void sampleStarted(SampleEvent sampleEvent) {
        //May be acceptable for fully async samplers?
    }

    @Override
    public void sampleStopped(SampleEvent sampleEvent) {
        //May be acceptable for fully async samplers?
    }

    @Override
    public void testStarted() {

        try {
            writer = new BufferedWriter(new FileWriter(getOutputFileName()));
        } catch (IOException e) {
            log.warn("Can't create output file " + getOutputFileName(), e);
            return;
        }

        secSet = new ConcurrentSkipListSet<Long>();
        threadsMap = new ConcurrentHashMap<Long, Integer>(getTimeout() + floatingSeconds);
        throughputMap = new ConcurrentHashMap<Long, AtomicInteger>(getTimeout() + floatingSeconds);
        sumLTMap = new ConcurrentHashMap<Long, AtomicLong>(getTimeout() + floatingSeconds);
        responseCodeMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicInteger>>(getTimeout() + floatingSeconds);
        sumInboundTraffic = new ConcurrentHashMap<Long, AtomicLong>(getTimeout() + floatingSeconds);
        sumOutboundTraffic = new ConcurrentHashMap<Long, AtomicLong>(getTimeout() + floatingSeconds);
        samplerTotalCounterMap = new ConcurrentHashMap<String, AtomicLong>();

        if(getPercentiles() != null) {
            ArgentumSecondRunnable.QUANTILES = getPercentiles();
            //For cumulative percentiles
            percentileDistMap = new ConcurrentHashMap<Long, AtomicLongArray>(getTimeout() + floatingSeconds);
            percentileDistShiftArray = new long[getTimeout()*1000 + 1];
            samplerPercentileDistMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicLongArray>>(getTimeout() + floatingSeconds);
            samplerCumulativePercentileShiftArray = new ConcurrentHashMap<String, long[]>();

        }
        if(getTimePeriods() != null) {
            ArgentumSecondRunnable.TIME_PERIODS = getTimePeriods();

            if(ArgentumSecondRunnable.TIME_PERIODS[ArgentumSecondRunnable.TIME_PERIODS.length - 1] < getTimeout()) {
                int new_time_periods[] = new int[ArgentumSecondRunnable.TIME_PERIODS.length + 1];
                //Hmmm. Sorry for that
                for(int i = 0; i < ArgentumSecondRunnable.TIME_PERIODS.length ;++i) {
                    new_time_periods[i] = ArgentumSecondRunnable.TIME_PERIODS[i];
                }
                new_time_periods[ArgentumSecondRunnable.TIME_PERIODS.length] = getTimeout();
                ArgentumSecondRunnable.TIME_PERIODS = new_time_periods;
            }
        }

        if(executors == null) {
            synchronized (this.getClass()) {
                if(executors == null) executors = Executors.newSingleThreadExecutor();
            }
        }
        started = true;
    }

    @Override
    public void testStarted(String s) {
        testStarted();
    }

    @Override
    public void testEnded() {
        while(secSet.size() > 0) {
            Long rSecond = secSet.pollFirst();

            if(executors != null) {
                executors.execute(new ArgentumSecondRunnable(rSecond, //second
                        threadsMap.get(rSecond),
                        throughputMap.get(rSecond).get(), //throughput
                        responseCodeMap.get(rSecond),
                        sumLTMap.get(rSecond).get(),
                        sumInboundTraffic.get(rSecond).get(),
                        sumOutboundTraffic.get(rSecond).get(),
                        true, //case distributions
                        percentileDistMap.get(rSecond),
                        percentileDistShiftArray,
                        samplerPercentileDistMap.get(rSecond),
                        samplerCumulativePercentileShiftArray,
                        samplerTotalCounterMap,
                        writer,
                        getTimeout()
                ));
            } else log.warn("Not found executors");

            threadsMap.remove(rSecond);
            throughputMap.remove(rSecond);
            sumLTMap.remove(rSecond);
            responseCodeMap.remove(rSecond);
            sumInboundTraffic.remove(rSecond);
            sumOutboundTraffic.remove(rSecond);
            percentileDistMap.remove(rSecond);
            samplerPercentileDistMap.remove(rSecond);
        }
        if(executors != null) {
            synchronized (this.getClass()) {
                executors.shutdown();
            }
        }
    }

    @Override
    public void testEnded(String s) {
        testEnded();
    }

    @Override
    public void testIterationStart(LoopIterationEvent loopIterationEvent) {
    }

    private String convertResponseCode(SampleResult sr) {
        if(!sr.getResponseCode().isEmpty()) {
            return sr.getResponseCode();
        } else {
            if(sr.isSuccessful())   return RC_OK;
            else                    return RC_ERROR;
        }

    }
}
