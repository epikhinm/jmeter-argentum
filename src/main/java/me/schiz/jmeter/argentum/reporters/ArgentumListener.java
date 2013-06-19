package me.schiz.jmeter.argentum.reporters;

import me.schiz.jmeter.argentum.Particle;
import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.engine.util.NoThreadClone;
import org.apache.jmeter.reporters.AbstractListenerElement;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleListener;
import org.apache.jmeter.testelement.TestListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
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
    protected ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicInteger>> titleMap; //Title -> Count map
    protected ConcurrentHashMap<Long, AtomicLong> sumRTMap;  //for average response time metric
    protected ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicLong>> sumRTSamplerMap;  //for average response time per sampler metric
    protected ConcurrentHashMap<Long, AtomicLong> sumLTMap;  //for average latency metric
    protected ConcurrentHashMap<Long, List<Particle>>   particlesMap; //full particles per second maps
    protected ConcurrentHashMap<Long, AtomicIntegerArray>   intervalDistMap; //interval distribution based on time periods per second map
    protected ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicIntegerArray>>   intervalDistSamplerMap; //interval distribution based on time periods per second map for all bullets
    protected ConcurrentHashMap<Long, AtomicLong> sumInboundTraffic; // for avg inbound traffic metric
    protected ConcurrentHashMap<Long, AtomicLong> sumOutboundTraffic; // for avg outbound traffic metric

    protected ConcurrentHashMap<Long, AtomicLongArray> percentileDistMap; //seconds map for cumulative percentile distribution
    protected long[]    percentileDistShiftArray; //shift-array for cumulative percentile distribution

    protected volatile boolean isCalcQuantileDist = false;
    protected volatile boolean isCalcIntervalDist = false;
    protected volatile boolean isCalcCumulativeQuantileDist = false;



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
            return null;
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
            return null;
        }
        isCalcIntervalDist = true;
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
            sumRTMap.put(second, new AtomicLong(0));
            sumRTSamplerMap.put(second, new ConcurrentHashMap<String, AtomicLong>());
            sumLTMap.put(second, new AtomicLong(0));
            responseCodeMap.put(second, new ConcurrentHashMap<String, AtomicInteger>());
            titleMap.put(second, new ConcurrentHashMap<String, AtomicInteger>());
            if(isCalcQuantileDist)  particlesMap.put(second, Collections.synchronizedList(new ArrayList<Particle>((int)(1.2 * max_th))));
            if(isCalcIntervalDist) {
                intervalDistMap.put(second, new AtomicIntegerArray(ArgentumSecondRunnable.TIME_PERIODS.length));
                intervalDistSamplerMap.put(second, new ConcurrentHashMap<String, AtomicIntegerArray>());
            }
            if(isCalcCumulativeQuantileDist) {
                percentileDistMap.put(second, new AtomicLongArray(getTimeout() * 1000 + 1));
            }
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
                        titleMap.get(rSecond),
                        sumRTMap.get(rSecond).get(),
                        sumRTSamplerMap.get(rSecond),
                        sumLTMap.get(rSecond).get(),
                        sumInboundTraffic.get(rSecond).get(),
                        sumOutboundTraffic.get(rSecond).get(),
                        isCalcQuantileDist ? particlesMap.get(rSecond) : null,
                        isCalcIntervalDist ? intervalDistMap.get(rSecond) : null,
                        isCalcIntervalDist ? intervalDistSamplerMap.get(rSecond) : null,
                        true, //case distributions
                        isCalcCumulativeQuantileDist ? percentileDistMap.get(rSecond) : null,
                        isCalcCumulativeQuantileDist ? percentileDistShiftArray : null,
                        writer
                ));
            } else log.warn("Not found executors");

            threadsMap.remove(rSecond);
            throughputMap.remove(rSecond);
            sumRTMap.remove(rSecond);
            sumRTSamplerMap.remove(rSecond);
            sumLTMap.remove(rSecond);
            responseCodeMap.remove(rSecond);
            titleMap.remove(rSecond);
            sumInboundTraffic.remove(rSecond);
            sumOutboundTraffic.remove(rSecond);
            if(isCalcQuantileDist)  particlesMap.remove(rSecond);
            if(isCalcIntervalDist) {
                intervalDistMap.remove(rSecond);
                intervalDistSamplerMap.remove(rSecond);
            }
            if(isCalcCumulativeQuantileDist) {
                percentileDistMap.remove(rSecond);
            }

        }
        return true;
    }

    private void addSamplerToSumRTSamplerMap(Long second, String sampler, int rt) {
        ConcurrentHashMap<String, AtomicLong> cursor = sumRTSamplerMap.get(second);
        if(cursor.get(sampler) == null) {
            // Sorry guys. It's really pain.
            synchronized (sumRTSamplerMap) {
                if(cursor.get(sampler) == null)  cursor.put(sampler, new AtomicLong(rt));
                else cursor.get(sampler).getAndAdd(rt);
            }
        } else cursor.get(sampler).getAndAdd(rt);
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

    private void addTitletoMap(Long second, String title) {
        ConcurrentHashMap<String, AtomicInteger> cursor = titleMap.get(second);
        if(cursor.get(title) == null) {
            synchronized (titleMap) {
                if(cursor.get(title) == null)  cursor.put(title, new AtomicInteger(1));
                else cursor.get(title).getAndIncrement();
            }
        } else cursor.get(title).incrementAndGet();
    }

    private void addToPidMap(Long second, String title, int rt) {
        ConcurrentHashMap<String, AtomicIntegerArray> cursor = intervalDistSamplerMap.get(second);
        if(cursor.get(title) == null) {
            synchronized (intervalDistSamplerMap) {
                if(cursor.get(title) == null)  cursor.put(title, new AtomicIntegerArray(ArgentumSecondRunnable.TIME_PERIODS.length));
                else {
                    for(int i = 0; i < ArgentumSecondRunnable.TIME_PERIODS.length ; ++i) {
                        if(rt < ArgentumSecondRunnable.TIME_PERIODS[i]) {
                            cursor.get(title).addAndGet(i, 1);
                            break;
                        }
                    }
                }
            }
        } else {
            for(int i = 0; i < ArgentumSecondRunnable.TIME_PERIODS.length ; ++i) {
                if(rt < ArgentumSecondRunnable.TIME_PERIODS[i]) {
                    cursor.get(title).addAndGet(i, 1);
                    break;
                }
            }
        }
    }

    @Override
    public void sampleOccurred(SampleEvent sampleEvent) {
        if(!started) return;
        //Only for quantile distribution
        Particle p = new Particle(sampleEvent.getResult());

        Long start = sampleEvent.getResult().getStartTime() / 1000;

        if(!secSet.contains(start)) {
            if(!createSecond(start)) {
                log.error("aggregation timeout");
                return;
            } else {
                log.info("second created");
            }
        }
        throughputMap.get(start).incrementAndGet();
        sumRTMap.get(start).getAndAdd(p.rt);
        sumLTMap.get(start).getAndAdd(sampleEvent.getResult().getLatency());
        addSamplerToSumRTSamplerMap(start, String.valueOf(p.name), p.rt);
        addRCtoMap(start, String.valueOf(p.rc));
        addTitletoMap(start, String.valueOf(p.name));
        sumInboundTraffic.get(start).getAndAdd(sampleEvent.getResult().getBodySize());
        sumOutboundTraffic.get(start).getAndAdd(sampleEvent.getResult().getHeadersSize());

        if(isCalcIntervalDist) {
            for(int i = 0; i < ArgentumSecondRunnable.TIME_PERIODS.length ; ++i) {
                if(p.rt < ArgentumSecondRunnable.TIME_PERIODS[i]) {
                    intervalDistMap.get(start).addAndGet(i, 1);
                    break;
                }
            }
            addToPidMap(start, String.valueOf(p.name), p.rt);
        }

        if(isCalcQuantileDist)  particlesMap.get(start).add(p);
        if(isCalcCumulativeQuantileDist)    percentileDistMap.get(start).getAndIncrement(p.rt);
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
        sumRTMap = new ConcurrentHashMap<Long, AtomicLong>(getTimeout() + floatingSeconds);
        sumRTSamplerMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicLong>>(getTimeout() + floatingSeconds);
        sumLTMap = new ConcurrentHashMap<Long, AtomicLong>(getTimeout() + floatingSeconds);
        responseCodeMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicInteger>>(getTimeout() + floatingSeconds);
        titleMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicInteger>>(getTimeout() + floatingSeconds);
        sumInboundTraffic = new ConcurrentHashMap<Long, AtomicLong>(getTimeout() + floatingSeconds);
        sumOutboundTraffic = new ConcurrentHashMap<Long, AtomicLong>(getTimeout() + floatingSeconds);

        if(getPercentiles() != null) {
            ArgentumSecondRunnable.QUANTILES = getPercentiles();
            isCalcQuantileDist = true;
            particlesMap = new ConcurrentHashMap<Long, List<Particle>>(getTimeout() + floatingSeconds);

            //For cumulative percentiles
            isCalcCumulativeQuantileDist = true;
            percentileDistMap = new ConcurrentHashMap<Long, AtomicLongArray>(getTimeout() + floatingSeconds);
            percentileDistShiftArray = new long[getTimeout()*1000 + 1];

        }
        if(getTimePeriods() != null) {
            ArgentumSecondRunnable.TIME_PERIODS = getTimePeriods();
            isCalcIntervalDist = true;
            intervalDistMap = new ConcurrentHashMap<Long, AtomicIntegerArray>(getTimeout() + floatingSeconds);
            intervalDistSamplerMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, AtomicIntegerArray>>(getTimeout() + floatingSeconds);

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
                        titleMap.get(rSecond),
                        sumRTMap.get(rSecond).get(),
                        sumRTSamplerMap.get(rSecond),
                        sumLTMap.get(rSecond).get(),
                        sumInboundTraffic.get(rSecond).get(),
                        sumOutboundTraffic.get(rSecond).get(),
                        isCalcQuantileDist ? particlesMap.get(rSecond) : null ,
                        isCalcIntervalDist ? intervalDistMap.get(rSecond) : null,
                        isCalcIntervalDist ? intervalDistSamplerMap.get(rSecond) : null,
                        isCalcIntervalDist, //case distributions
                        isCalcCumulativeQuantileDist ? percentileDistMap.get(rSecond) : null,
                        isCalcCumulativeQuantileDist ? percentileDistShiftArray : null,
                        writer
                ));
            } else log.warn("Not found executors");

            threadsMap.remove(rSecond);
            throughputMap.remove(rSecond);
            sumRTMap.remove(rSecond);
            sumRTSamplerMap.remove(rSecond);
            sumLTMap.remove(rSecond);
            responseCodeMap.remove(rSecond);
            titleMap.remove(rSecond);
            sumInboundTraffic.remove(rSecond);
            sumOutboundTraffic.remove(rSecond);
            if(isCalcQuantileDist)  particlesMap.remove(rSecond);
            if(isCalcIntervalDist) {
                intervalDistMap.remove(rSecond);
                intervalDistSamplerMap.remove(rSecond);
            }
            if(isCalcCumulativeQuantileDist) {
                percentileDistMap.remove(rSecond);
            }
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
}
