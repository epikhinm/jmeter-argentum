package me.schiz.jmeter.argentum.reporters;

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
    protected HashMap<String, Long> titleMap;
    protected HashMap<String, AtomicLong> sumRTSamplerMap;
    protected long sumRT;
    protected long sumLT;
    protected long inbound;
    protected long outbound;
    protected boolean infoCase;
    protected Writer writer;
    protected AtomicLongArray percentileDistArray;
    protected long[] percentileShiftArray;

    protected ConcurrentHashMap<String, AtomicLongArray> samplerPercentileDistMap;
    protected ConcurrentHashMap<String, long[]> samplerCumulativeShiftArrayMap;
    protected ConcurrentHashMap<String, AtomicLong> samplerTotalCounterMap;

    protected int timeout;

    static long totalResponseCounter;   //deprecated

    static float[] QUANTILES = null;
    static int[] TIME_PERIODS = null;
    static float[] DEFAULT_QUANTILES = {0.25f, 0.5f, 0.75f, 0.8f, 0.9f, 0.95f, 0.98f, 0.99f, 1.0f};
    static int[] DEFAULT_TIME_PERIODS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 11000};

    public ArgentumSecondRunnable(long second,
                                  int active_threads,
                                  int throughput,
                                  ConcurrentHashMap<String, AtomicInteger> responseCodeMap,
                                  long sumLT,
                                  long inbound,
                                  long outbound,
                                  boolean infoCase,
                                  AtomicLongArray percentileDistArray, //for second percentile distribution
                                  long[] percentileShiftArray, // for overall percentile distribution
                                  ConcurrentHashMap<String, AtomicLongArray> samplerPercentileDistMap,
                                  ConcurrentHashMap<String, long[]> samplerCumulativeShiftArray,
                                  ConcurrentHashMap<String, AtomicLong> samplerTotalCounterMap,
                                  Writer writer,
                                  int timeout) {
        this.second = second;
        this.active_threads = active_threads;
        this.throughput = throughput;
        this.responseCodeMap = responseCodeMap;
        this.titleMap = new HashMap<String, Long>();
        this.sumRT = 0;
        this.sumRTSamplerMap = new HashMap<String, AtomicLong>();
        this.sumLT = sumLT;
        this.inbound = inbound;
        this.outbound = outbound;
        this.infoCase = infoCase;

        if(percentileDistArray != null) {
            this.percentileDistArray = percentileDistArray;
            this.percentileShiftArray = percentileShiftArray;
        }

        this.samplerPercentileDistMap = samplerPercentileDistMap;
        this.samplerCumulativeShiftArrayMap = samplerCumulativeShiftArray;
        this.samplerTotalCounterMap = samplerTotalCounterMap;

        this.writer = writer;
        this.timeout = timeout;
    }

    private JSONObject calculateSecondTotalPercentile() {
        JSONObject result = new JSONObject();

        long i_rCount;
        long[] generalShiftArray = new long[timeout*1000 + 1];
        for(int i = 0; i < percentileDistArray.length() ; ++i) {
            i_rCount = percentileDistArray.get(i);
            if(i_rCount > 0) {
                totalResponseCounter += i_rCount;
                for(int j = i; j < generalShiftArray.length; ++j)  {
                    generalShiftArray[j] += i_rCount; //What about SSE?:)
                }
                for(int j=i;j<percentileShiftArray.length;++j) {
                    percentileShiftArray[j] += i_rCount; // for cumulative distribution
                }
                sumRT += i_rCount * i;
            }
        }

        for(float f: QUANTILES) {
            result.put(f * 100, binarySearchMinIndex(generalShiftArray, f));
        }

        return result;
    }

    private JSONObject calculateCumulativeTotalPercentile() {
        JSONObject result = new JSONObject();
        for(float f: QUANTILES) {
            result.put(f * 100, binarySearchMinIndex(this.percentileShiftArray,  f));
        }
        return result;
    }

    private JSONObject calculateSecondSamplerPercentile() {
        JSONObject result = new JSONObject();

        for(String sampler : samplerPercentileDistMap.keySet()) {
            JSONObject samplerPercentile = new JSONObject();

            AtomicLongArray samplerDistribution = samplerPercentileDistMap.get(sampler);
            long[] cumulativeShiftArray = samplerCumulativeShiftArrayMap.get(sampler);
            AtomicLong samplerCounter = samplerTotalCounterMap.get(sampler);

            if(cumulativeShiftArray == null) {
                cumulativeShiftArray = new long[timeout*1000 + 1];
                Arrays.fill(cumulativeShiftArray, Long.valueOf(0));
                samplerCumulativeShiftArrayMap.put(sampler, cumulativeShiftArray);
            }
            if(samplerCounter == null) {
                samplerCounter = new AtomicLong(0);
                samplerTotalCounterMap.put(sampler, samplerCounter);
            }

            long samplerThroughput = 0;
            long i_rCount;
            long[] samplerShiftArray = new long[timeout*1000 + 1];
            AtomicLong sumRTSamplerCounter = sumRTSamplerMap.get(sampler);
            if(sumRTSamplerCounter == null) {
                sumRTSamplerCounter = new AtomicLong(0);
                sumRTSamplerMap.put(sampler, sumRTSamplerCounter);
            }
            for(int i = 0; i < samplerDistribution.length() ; ++i) {
                i_rCount = samplerDistribution.get(i);
                if(i_rCount > 0) {
                    for(int j = i; j < samplerShiftArray.length; ++j)  {
                        samplerShiftArray[j] += i_rCount; //What about SSE?:)
                    }
                    for(int j = i; j < cumulativeShiftArray.length; ++j) {
                        cumulativeShiftArray[j] += i_rCount;    //SSE ?
                    }
                    samplerThroughput += i_rCount;
                    samplerCounter.getAndAdd(i_rCount);
                    sumRTSamplerMap.get(sampler) .addAndGet(i_rCount * i);
                }
            }
            this.titleMap.put(sampler, samplerThroughput);
            for(float f: QUANTILES) {
                samplerPercentile.put(f * 100, binarySearchMinIndex(samplerShiftArray, f));
            }
            result.put(sampler, samplerPercentile);
        }

        return result;
    }

    public JSONObject calculateCumulativeSamplerPercentile() {
        JSONObject result = new JSONObject();
        for(String sampler : samplerPercentileDistMap.keySet()) {
            JSONObject samplerCumulativePercentile = new JSONObject();
            for(float f: QUANTILES) {
                samplerCumulativePercentile.put(f * 100, binarySearchMinIndex(this.samplerCumulativeShiftArrayMap.get(sampler), f));
            }
            result.put(sampler, samplerCumulativePercentile);
        }
        return result;
    }

    private ArrayList<JSONObject> calculateSecondTotalIntervalDistribution() {
        long sum;
        int prev = 0;
        ArrayList<JSONObject> distList = new ArrayList<JSONObject>(TIME_PERIODS.length);
        for(int i=0; i<TIME_PERIODS.length;++i) {
            sum = 0;
            for(int j=prev+1; j< TIME_PERIODS[i];j++) {
                sum += this.percentileDistArray.get(j); //SSE ?
            }
            JSONObject interval = new JSONObject();
            interval.put("from", prev);
            interval.put("to", TIME_PERIODS[i]);
            interval.put("count", sum);
            prev = TIME_PERIODS[i];
            distList.add(interval);
        }
        return distList;
    }

    private JSONObject calculateSamplerSecondTotalIntervalDistribution() {
        JSONObject result = new JSONObject();
        for(String sampler : samplerPercentileDistMap.keySet()) {
            ArrayList<JSONObject> distList = new ArrayList<JSONObject>(TIME_PERIODS.length);
            long sum;
            int prev = 0;
            for(int i=0; i<TIME_PERIODS.length;++i) {
                sum = 0;
                for(int j=prev+1; j< TIME_PERIODS[i];j++) {
                    sum += this.samplerPercentileDistMap.get(sampler).get(j);
                }
                JSONObject interval = new JSONObject();
                interval.put("from", prev);
                interval.put("to", TIME_PERIODS[i]);
                interval.put("count", sum);
                prev = TIME_PERIODS[i];
                distList.add(interval);
            }
            result.put(sampler, distList);
        }
        return result;
    }

    @Override
    public void run() {
        try {
            JSONObject jsonSecond = new JSONObject();

            jsonSecond.put("second", second);
            jsonSecond.put("th", throughput);

            jsonSecond.put("avg_lt", (sumLT / throughput));
            jsonSecond.put("active_threads", active_threads);

            jsonSecond.put("rc", responseCodeMap);

            jsonSecond.put("samplers", titleMap);

            JSONObject jsonTraffic = new JSONObject();
            jsonTraffic.put("inbound", inbound);
            jsonTraffic.put("outbound", outbound);
            jsonTraffic.put("avg_response_size", inbound / throughput);
            jsonTraffic.put("avg_request_size", outbound / throughput);

            jsonSecond.put("traffic", jsonTraffic);

            if(TIME_PERIODS != null) {
                log.info("calculate interval distribution");
                jsonSecond.put("interval_dist", calculateSecondTotalIntervalDistribution());
                jsonSecond.put("sampler_interval_dist", calculateSamplerSecondTotalIntervalDistribution());
            }

            if(QUANTILES != null) {
                log.warn("calculate percentiles");
                jsonSecond.put("percentile", calculateSecondTotalPercentile());
                jsonSecond.put("sampler_percentile", calculateSecondSamplerPercentile());
                jsonSecond.put("cumulative_percentile", calculateCumulativeTotalPercentile());
                jsonSecond.put("cumulative_sampler_percentile", calculateCumulativeSamplerPercentile());
            }

            LinkedHashMap<String, Long> samplerAvgRTMap = new LinkedHashMap<String, Long>();
            for(String sample : sumRTSamplerMap.keySet()) {
                samplerAvgRTMap.put(sample, sumRTSamplerMap.get(sample).get() / titleMap.get(sample));
            }
            jsonSecond.put("sampler_avg_rt", samplerAvgRTMap);
            jsonSecond.put("avg_rt", (sumRT / throughput));

            writer.write((jsonSecond.toJSONString() + "\n" ).toCharArray());
            writer.flush();
        } catch(Exception e) {
            log.error("Runnable exception", e);
            log.error(e.getMessage());
        }
    }

    private static long binarySearchMinIndex(long []array, float f) {
        int left = 0, right = array.length-1;
        int half_sum;
        long x = (long)(array[right] * f);
        if(x > array[right])    x = array[right];   //for case when x * 1.0f > x
        while (right - left > 1 ) {
            half_sum = (right + left) / 2;
            if(array[half_sum] >= x) right = half_sum;
            else if(array[half_sum] < x) left = half_sum;
        }
        return right;
    }
}
