package me.schiz.jmeter.argentum.reporters;

import org.apache.jmeter.engine.util.NoThreadClone;
import org.apache.jmeter.reporters.AbstractListenerElement;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleListener;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPOutputStream;

public class ArgentumListener extends AbstractListenerElement
        implements SampleListener, NoThreadClone, TestStateListener {

    private static final Logger log  = LoggingManager.getLoggerForClass();

    private static LinkedList<ArgentumListener> argentumListeners;
    public final static int floatingSeconds = 2;
    public static String outputFileName = "ArgentumListener.OutputFileName";
    public static String timeout = "ArgentumListener.timeout";
    public static String percentiles = "ArgentumListener.percentiles";
    public static String timePeriods = "ArgentumListener.timePeriods";
    public static String rebuildCumulative = "ArgentumListener.rebuildCumulative";
    public static String pdf = "ArgentumListener.pdf";
    public static String cdf = "ArgentumListener.cdf";

    protected OutputStream outputStream;
    protected PrintWriter printWriter;

    //runnable needs public vars:(
    public int timeout_value;
    public ConcurrentSkipListSet<Long> secSet;
    public ConcurrentHashMap<Long, Integer> threadsMap; //for active_threads metric
    public ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicInteger>> responseCodeMap;   //ReturnCode -> Count per second map
    public ConcurrentHashMap<Long, AtomicLong> sumLTMap;  //for average latency metric
    public ConcurrentHashMap<Long, AtomicLong> sumInboundTraffic; // for avg inbound traffic metric
    public ConcurrentHashMap<Long, AtomicLong> sumOutboundTraffic; // for avg outbound traffic metric

    public ConcurrentHashMap<Long, AtomicLongArray> lastPDF; //PDF for last N seconds
    public long[]   totalOverallCDF;
    public ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicLongArray>> lastSamplerPDF;
    public ConcurrentHashMap<String, long[]>    totalSamplerCDF;

    public ConcurrentHashMap<Long, ReentrantReadWriteLock> rwLockMap;

    public HashMap<String, Object> lastMetrics;

    private boolean started = false;

    static String RC_OK = "200";
    static String RC_ERROR = "500";

    protected static ScheduledExecutorService executors;

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
            return ScheduledArgentumRunnable.DEFAULT_QUANTILES;
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
            for(String time: times) {
                time_periods[j++] = Integer.parseInt(time);
            }
        } catch (NumberFormatException nfe) {
            return ScheduledArgentumRunnable.DEFAULT_TIME_PERIODS;
        }
        return time_periods;
    }
    public void setTimeout(int tmt) {
        setProperty(timeout, tmt);
        if(tmt != timeout_value)    timeout_value = tmt;
    }
    public int getTimeout() {
        return getPropertyAsInt(timeout);
    }

    public void setRebuildCumulative(boolean flag) {
        setProperty(rebuildCumulative, flag);
    }
    public boolean isRebuildCumulative() {
        return getPropertyAsBoolean(rebuildCumulative);
    }

    public void setPDF(boolean use) {
        setProperty(pdf, use);
    }
    public boolean isEnablePDF() {
        return getPropertyAsBoolean(pdf);
    }

    public void setCDF(boolean use) {
        setProperty(cdf, use);
    }
    public boolean isEnableCDF() {
        return getPropertyAsBoolean(cdf);
    }

    private boolean createSecond(Long second) {
        synchronized (this) {
            if(secSet.contains(second))    return true; //double check
            if(secSet.size() > 0) {
                if(secSet.first() > second)    return false; // very old SampleResult. Sorry
            }
            threadsMap.put(second, JMeterContextService.getNumberOfThreads());
            sumLTMap.put(second, new AtomicLong(0));
            responseCodeMap.put(second, new ConcurrentHashMap<String, AtomicInteger>());
            lastPDF.put(second, new AtomicLongArray(timeout_value * 1000 + 1));
            lastSamplerPDF.put(second, new ConcurrentHashMap<String, AtomicLongArray>());
            sumInboundTraffic.put(second, new AtomicLong(0));
            sumOutboundTraffic.put(second, new AtomicLong(0));
            rwLockMap.put(second, new ReentrantReadWriteLock());

            secSet.add(second);
        }
        return true;
    }

    private void addRCtoMap(Long second, String rc) {
        ConcurrentHashMap<String, AtomicInteger> cursor = responseCodeMap.get(second);
        if(cursor.get(rc) == null) {
            synchronized (responseCodeMap) {
                if(cursor.get(rc) == null)  cursor.put(rc, new AtomicInteger(0));
            }
        }
        cursor.get(rc).incrementAndGet();
    }

    private void addToSamplerDistMap(Long second, String title, int rt) {
        ConcurrentHashMap<String, AtomicLongArray> cursor = lastSamplerPDF.get(second);
        if(cursor.get(title) == null) {
            synchronized (lastSamplerPDF) {
                if(cursor.get(title) == null) {
                    cursor.put(title, new AtomicLongArray(timeout_value * 1000 + 1));
                }
            }
        }
        cursor.get(title).incrementAndGet(rt);
    }

    public static void sampleOccured(SampleEvent sampleEvent) {
        for(ArgentumListener listener : argentumListeners) {
            listener.sampleOccurred(sampleEvent);
        }
    }

    @Override
    public void sampleOccurred(SampleEvent sampleEvent) {
        if(!started) return;
        long now = System.currentTimeMillis() / 1000;
        SampleResult sr = sampleEvent.getResult();

        long second = sr.getEndTime() / 1000;
        String samplerName = sr.getSampleLabel();
        int rt = (int)sr.getTime();

        //flush not ended samplers
        if(second == 0) {
            //SampleResult without ResponseCode, without response time.
            log.warn("not ended sample");
            return;
        }
        if(now > second + timeout_value || rt > timeout_value * 1000) {
            log.error("aggregation timeout, sampleEnd: " + second + ", now: " +now+" rt: " + rt + "ms, timeout: " + timeout_value * 1000);
            return;
        }

        if(!secSet.contains(second)) {
            if(!createSecond(second)) {
                log.error("aggregation timeout, sampleEnd: " + second + " not in [" + this.secSet.first() + ";" + this.secSet.last()+ "]");
                return;
            }
        }
        try{
            // Inversed read-write lock. We can do concurrency writes, and only single read

            ReentrantReadWriteLock.ReadLock writeLock = rwLockMap.get(second).readLock();
            writeLock.lock();
            if(!secSet.contains(second)) {
                if(!createSecond(second)) {
                    log.error("aggregation timeout, sampleEnd: " + second + " not in [" + this.secSet.first() + ";" + this.secSet.last()+ "]");
                    writeLock.unlock();
                    return;
                }
            }
            lastPDF.get(second).incrementAndGet(rt);
            addToSamplerDistMap(second, samplerName, rt);
            addRCtoMap(second, convertResponseCode(sr));
            sumLTMap.get(second).getAndAdd(sr.getLatency());
            sumInboundTraffic.get(second).getAndAdd(sr.getBodySize());
            sumOutboundTraffic.get(second).getAndAdd(sr.getHeadersSize());
            writeLock.unlock();
        } catch (NullPointerException npe) {
            log.error("aggregation timeout ", npe);
        }
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
        if(argentumListeners == null) {
            synchronized (this.getClass()) {
                if(argentumListeners == null) {
                    argentumListeners = new LinkedList<ArgentumListener>();
                }
            }
        }

        synchronized (argentumListeners) {
            argentumListeners.add(this);
        }

        try {
            if(getOutputFileName().endsWith("gzip") || getOutputFileName().endsWith("gz")) {
                outputStream = new GZIPOutputStream(new FileOutputStream(getOutputFileName()),
                        true); //enable sync flush
            } else {
                outputStream = new FileOutputStream(getOutputFileName());
            }
            printWriter = new PrintWriter(outputStream,
                    true); //enable autoflush

            timeout_value = getTimeout();
            secSet = new ConcurrentSkipListSet<Long>();
            threadsMap = new ConcurrentHashMap<Long, Integer>(timeout_value + floatingSeconds);
            sumLTMap = new ConcurrentHashMap<Long, AtomicLong>(timeout_value + floatingSeconds);
            responseCodeMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicInteger>>(timeout_value + floatingSeconds);
            sumInboundTraffic = new ConcurrentHashMap<Long, AtomicLong>(timeout_value + floatingSeconds);
            sumOutboundTraffic = new ConcurrentHashMap<Long, AtomicLong>(timeout_value + floatingSeconds);
            rwLockMap = new ConcurrentHashMap<Long, ReentrantReadWriteLock>(timeout_value + floatingSeconds);

            lastMetrics = new HashMap<String, Object>();

            if(getPercentiles() != null) {
                ScheduledArgentumRunnable.QUANTILES = getPercentiles();
                //For cumulative percentiles
                lastPDF = new ConcurrentHashMap<Long, AtomicLongArray>(timeout_value + floatingSeconds);
                totalOverallCDF = new long[timeout_value*1000 + 1];
                lastSamplerPDF = new ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicLongArray>>(timeout_value + floatingSeconds);
                totalSamplerCDF = new ConcurrentHashMap<String, long[]>();
            }
            if(getTimePeriods() != null) {
                ScheduledArgentumRunnable.TIME_PERIODS = getTimePeriods();

                if(ScheduledArgentumRunnable.TIME_PERIODS[ScheduledArgentumRunnable.TIME_PERIODS.length - 1] < timeout_value) {
                    int new_time_periods[] = new int[ScheduledArgentumRunnable.TIME_PERIODS.length + 1];
                    System.arraycopy(ScheduledArgentumRunnable.TIME_PERIODS, 0, new_time_periods, 0, ScheduledArgentumRunnable.TIME_PERIODS.length);
                    new_time_periods[ScheduledArgentumRunnable.TIME_PERIODS.length] = timeout_value;
                    ScheduledArgentumRunnable.TIME_PERIODS = new_time_periods;
                }
            }

            if(executors == null) {
                synchronized (this.getClass()) {
                    if(executors == null)   executors = Executors.newScheduledThreadPool(1);
                    executors.scheduleAtFixedRate(new ScheduledArgentumRunnable(this, printWriter, isRebuildCumulative(), isEnablePDF(), isEnableCDF(), lastMetrics), 0, 50, TimeUnit.MILLISECONDS);
                }
            }

            log.info("You are using argentum. Be careful.");
            started = true;
        } catch (IOException e) {
            log.warn("Can't create output file " + getOutputFileName(), e);
            started = false;
            return;
        }
    }

    @Override
    public void testStarted(String s) {
        testStarted();
    }

    @Override
    public void testEnded() {
        started = false;
        while(secSet.size() > 0) {
            //last second may be dropped
        }
        if(executors != null) {
            synchronized (this.getClass()) {
                executors.shutdown();
            }
        }

        if(!argentumListeners.isEmpty()) {
            synchronized (argentumListeners) {
                if(!argentumListeners.isEmpty())  argentumListeners.clear();
            }
        }

        printWriter.close();
        try {
            outputStream.close();
        } catch (IOException e) {
            log.warn("teardown exception", e);
        }
        lastMetrics.clear();
    }

    @Override
    public void testEnded(String s) {
        testEnded();
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
