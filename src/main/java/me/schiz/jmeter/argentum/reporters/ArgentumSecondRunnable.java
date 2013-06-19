package me.schiz.jmeter.argentum.reporters;

import me.schiz.jmeter.argentum.Particle;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.json.simple.JSONObject;

import java.io.Writer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class ArgentumSecondRunnable implements Runnable {

    private static final Logger log = LoggingManager.getLoggerForClass();

    protected long second;
    protected int active_threads;
    protected int throughput;
    protected ConcurrentHashMap<String, AtomicInteger> responseCodeMap;
    protected ConcurrentHashMap<String, AtomicInteger> titleMap;
    protected ConcurrentHashMap<String, AtomicLong> sumRTSamplerMap;
    protected long sumRT;
    protected long sumLT;
    protected long inbound;
    protected long outbound;
    protected List<Particle> particleList;
    protected AtomicIntegerArray intervalDistribution;
    ConcurrentHashMap<String, AtomicIntegerArray> intervalDistSamplerMap;
    protected boolean infoCase;
    protected Writer writer;
    protected AtomicLongArray percentileDistArray;
    protected long[] percentileShiftArray;

    static long totalResponseCounter;

    static float[] QUANTILES = null;
    static int[] TIME_PERIODS = null;
    static float[] DEFAULT_QUANTILES = {0.25f, 0.5f, 0.75f, 0.8f, 0.9f, 0.95f, 0.98f, 0.99f, 1.0f};
    static int[] DEFAULT_TIME_PERIODS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 11000};

    public ArgentumSecondRunnable(long second,
                                  int active_threads,
                                  int throughput,
                                  ConcurrentHashMap<String, AtomicInteger> responseCodeMap,
                                  ConcurrentHashMap<String, AtomicInteger> titleMap,
                                  long sumRT,
                                  ConcurrentHashMap<String, AtomicLong> sumRTSamplerMap,
                                  long sumLT,
                                  long inbound,
                                  long outbound,
                                  List<Particle> particleList,
                                  AtomicIntegerArray intervalDistribution,
                                  ConcurrentHashMap<String, AtomicIntegerArray> intervalDistSamplerMap,
                                  boolean infoCase,
                                  AtomicLongArray percentileDistArray,
                                  long[] percentileShiftArray,
                                  Writer writer) {
        this.second = second;
        this.active_threads = active_threads;
        this.throughput = throughput;
        this.responseCodeMap = responseCodeMap;
        this.titleMap = titleMap;
        this.sumRT = sumRT;
        this.sumRTSamplerMap = sumRTSamplerMap;
        this.sumLT = sumLT;
        this.inbound = inbound;
        this.outbound = outbound;
        this.particleList = particleList;
        this.intervalDistribution = intervalDistribution;
        this.intervalDistSamplerMap = intervalDistSamplerMap;
        this.infoCase = infoCase;

        if(percentileDistArray != null) {
            this.percentileDistArray = percentileDistArray;
            this.percentileShiftArray = percentileShiftArray;
        }

        this.writer = writer;
    }

    @Override
    public void run() {
        try {
            JSONObject jsonSecond = new JSONObject();

            boolean do_really_case_info = false;

            jsonSecond.put("second", second);
            jsonSecond.put("th", throughput);
            jsonSecond.put("avg_rt", (sumRT / throughput));

            LinkedHashMap<String, Long> samplerAvgRTMap = new LinkedHashMap<String, Long>();
            for(String sample : sumRTSamplerMap.keySet()) {
                samplerAvgRTMap.put(sample, sumRTSamplerMap.get(sample).get() / titleMap.get(sample).get());
            }

            jsonSecond.put("sampler_avg_rt", samplerAvgRTMap);
            jsonSecond.put("avg_lt", (sumLT / throughput));
            jsonSecond.put("active_threads", active_threads);

            jsonSecond.put("rc", responseCodeMap);

            if(titleMap.size() > 1 && infoCase)    do_really_case_info = true;
            jsonSecond.put("samplers", titleMap);

            JSONObject jsonTraffic = new JSONObject();
            jsonTraffic.put("inbound", inbound);
            jsonTraffic.put("outbound", outbound);
            jsonTraffic.put("avg_response_size", inbound / throughput);
            jsonTraffic.put("avg_request_size", outbound / throughput);

            jsonSecond.put("traffic", jsonTraffic);

            JSONObject percentile_pair;

            if(TIME_PERIODS != null) {
                log.info("calculate interval distribution");
                int prev = 0;
                ArrayList<JSONObject> summaryID = new ArrayList<JSONObject>(TIME_PERIODS.length);
                for(int i = 0; i < TIME_PERIODS.length ; ++i) {
                    JSONObject interval = new JSONObject();
                    interval.put("from", prev);
                    interval.put("to", TIME_PERIODS[i]);
                    interval.put("count", intervalDistribution.get(i));
                    summaryID.add(interval);

                    prev = TIME_PERIODS[i];
                }
                jsonSecond.put("interval_dist", summaryID);


                JSONObject fullPID = new JSONObject();
                for(String title : intervalDistSamplerMap.keySet()) {
                    ArrayList<JSONObject> titleID = new ArrayList<JSONObject>(TIME_PERIODS.length);
                    prev = 0;
                    for(int i = 0; i < TIME_PERIODS.length ; ++i) {
                        JSONObject interval = new JSONObject();
                        interval.put("from", prev);
                        interval.put("to", TIME_PERIODS[i]);
                        interval.put("count", intervalDistribution.get(i));
                        titleID.add(interval);
                        prev = TIME_PERIODS[i];
                    }
                    fullPID.put(title, titleID);
                }
                jsonSecond.put("sampler_interval_dist", fullPID);
            }

            if(QUANTILES != null) {
                log.warn("calculate percentiles");
                // One slow mega sort
                Collections.sort(particleList);

                JSONObject commonPerc = new JSONObject();
                for(float f : QUANTILES) {
                    commonPerc.put(f*100, particleList.get((int)(f*throughput - 1)).rt);
                }
                jsonSecond.put("percentile", commonPerc);

                if(do_really_case_info) {
                    LinkedHashMap<String, ArrayList<Particle>> caseParticles = new LinkedHashMap<String, ArrayList<Particle>>();
                    JSONObject fullPerc = new JSONObject(); // for all cases

                    for(String t : titleMap.keySet()) {
                        caseParticles.put(t, new ArrayList<Particle>(titleMap.get(t).get()));
                    }

                    for(Particle p : particleList) {
                        caseParticles.get(String.valueOf(p.name)).add(p);
                    }

                    for(String t : titleMap.keySet()) {
                        int case_th = titleMap.get(t).get();
                        JSONObject casePerc = new JSONObject();
                        for(float f: QUANTILES) {
                            casePerc.put(f * 100, caseParticles.get(t).get((int) (f * case_th - 1)).rt);
                        }
                        fullPerc.put(t, casePerc);
                    }
                    jsonSecond.put("sampler_percentile", fullPerc);
                } else {
                    //if only one title copy information from main percentile map;
                    JSONObject fullPerc = new JSONObject();
                    for(String t : titleMap.keySet()) {
                        fullPerc.put(t, commonPerc);
                        break;
                    }
                    jsonSecond.put("sampler_percentile", fullPerc);
                }
                if(percentileDistArray != null) {
                    long i_rCount = 0;
                    for(int i = 0; i < percentileDistArray.length() ; ++i) {
                        i_rCount = percentileDistArray.get(i);
                        if(i_rCount > 0) {
                            totalResponseCounter += i_rCount;
                            for(int j = i; j < percentileShiftArray.length; ++j)  {
                                percentileShiftArray[j] += i_rCount; //What about SSE?:)
                            }
                        }
                    }

                    JSONObject cumulativePercentiles = new JSONObject();
                    for(float f: QUANTILES) {
                        cumulativePercentiles.put(f * 100, binarySearchMinIndex(percentileShiftArray, (int)(totalResponseCounter * f)));
                    }
                    jsonSecond.put("cumulative_percentile", cumulativePercentiles);
                }
            }
            writer.write((jsonSecond.toJSONString() + "\n" ).toCharArray());
            writer.flush();
        } catch(Exception e) {
            log.error("Runnable exception", e);
            log.error(e.getStackTrace().toString());
        }
    }

    private static long binarySearchMinIndex(long []array, int x) {
        int left = 0, right = array.length-1;
        int half_sum;
        while (right - left > 1 ) {
            half_sum = (right + left) / 2;
            if(array[half_sum] >= x) right = half_sum;
            else if(array[half_sum] < x) left = half_sum;
        }
        return right;
    }
}
