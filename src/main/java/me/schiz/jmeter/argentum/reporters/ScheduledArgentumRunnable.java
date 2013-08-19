package me.schiz.jmeter.argentum.reporters;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.json.simple.JSONObject;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class ScheduledArgentumRunnable implements Runnable {

    private static final Logger log = LoggingManager.getLoggerForClass();

    protected ArgentumListener listener;
    protected long second;
    protected int active_threads;
    protected int throughput;
    protected ConcurrentHashMap<String, AtomicInteger> responseCodeMap;
    protected HashMap<String, Integer> titleMap;
    protected HashMap<String, AtomicLong> sumRTSamplerMap;
    protected long sumRT;
    protected long sumLT;
    protected long inbound;
    protected long outbound;
    protected Writer writer;
    protected AtomicLongArray percentileDistArray;
    protected long[] percentileShiftArray;

    protected ConcurrentHashMap<String, AtomicLongArray> samplerPercentileDistMap;
    protected ConcurrentHashMap<String, long[]> samplerCumulativeShiftArrayMap;
    protected ConcurrentHashMap<String, AtomicLong> samplerTotalCounterMap;

    protected int timeout;

    static int append_agg_timeout = 2; //2s

    static float[] QUANTILES = null;
    static int[] TIME_PERIODS = null;
    static float[] DEFAULT_QUANTILES = {0.25f, 0.5f, 0.75f, 0.8f, 0.9f, 0.95f, 0.98f, 0.99f, 1.0f};
    static int[] DEFAULT_TIME_PERIODS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 11000};

    public ScheduledArgentumRunnable(ArgentumListener listener, Writer writer) {
        this.listener = listener;
        this.writer = writer;
        this.timeout = this.listener.getTimeout();
    }

    public boolean check() {
        if(this.listener.secSet.first() + this.listener.getTimeout() + append_agg_timeout > (System.currentTimeMillis() / 1000)) {
            log.debug("failure check, first " + this.listener.secSet.first() + " + timeout " + this.listener.getTimeout() + "> " + (System.currentTimeMillis() / 1000));
            return false;
        } else {
            log.debug("time: " + (System.currentTimeMillis() / 1000) + " queue: " + this.listener.secSet.toString());

            this.second = this.listener.secSet.pollFirst();
            this.active_threads = this.listener.threadsMap.get(this.second);
            this.throughput = 1;
            this.responseCodeMap = this.listener.responseCodeMap.get(this.second);
            this.titleMap = new HashMap<String, Integer>();
            this.sumRTSamplerMap = new HashMap<String, AtomicLong>();
            this.sumRT = 0;
            this.sumLT = this.listener.sumLTMap.get(this.second).get();
            this.inbound = this.listener.sumInboundTraffic.get(this.second).get();
            this.outbound = this.listener.sumOutboundTraffic.get(this.second).get();
            this.percentileDistArray = this.listener.percentileDistMap.get(this.second);
            this.percentileShiftArray = this.listener.percentileDistShiftArray;
            this.samplerPercentileDistMap = this.listener.samplerPercentileDistMap.get(this.second);
            this.samplerCumulativeShiftArrayMap = this.listener.samplerCumulativePercentileShiftArray;
            this.samplerTotalCounterMap = this.listener.samplerTotalCounterMap;

            return true;
        }
    }

    public void delete() {
        this.listener.threadsMap.remove(this.second);
        this.listener.sumLTMap.remove(this.second);
        this.listener.responseCodeMap.remove(this.second);
        this.listener.sumInboundTraffic.remove(this.second);
        this.listener.sumOutboundTraffic.remove(this.second);
        this.listener.percentileDistMap.remove(this.second);
        this.listener.samplerPercentileDistMap.remove(this.second);
        this.listener.samplerTotalCounterMap.remove(this.second);
        samplerPercentileDistMap = null;
    }

    private HashMap<String, Integer> calculateSecondTotalPercentile() {
        HashMap<String, Integer> result = new HashMap(QUANTILES.length);

        long i_rCount;
        long[] generalShiftArray = new long[timeout*1000 + 1];
        for(int i = 0; i < percentileDistArray.length() ; ++i) {
            i_rCount = percentileDistArray.get(i);
            if(i_rCount > 0) {
                this.throughput += i_rCount;
                for(int j=i; j < generalShiftArray.length; ++j)  {
                    generalShiftArray[j] += i_rCount; //What about SSE?:)
                    percentileShiftArray[j] += i_rCount; // for cumulative distribution
                }
                sumRT += i_rCount * i;
            }
        }

        for(float f: QUANTILES) {
            result.put(String.valueOf(f * 100), (int) binarySearchMinIndex(generalShiftArray, f));
        }

        return result;
    }

    private HashMap<String, Integer> calculateCumulativeTotalPercentile() {
        HashMap<String, Integer> result = new HashMap<String, Integer>(QUANTILES.length);
        for(float f: QUANTILES) {
            result.put(String.valueOf(f * 100), (int)binarySearchMinIndex(this.percentileShiftArray,  f));
        }
        return result;
    }

    private HashMap<String, HashMap<String, Integer>> calculateSecondSamplerPercentile() {
        HashMap<String, HashMap<String, Integer>> result = new HashMap<String, HashMap<String, Integer>>(samplerPercentileDistMap.size());

        long zero = 0;

        for(String sampler : samplerPercentileDistMap.keySet()) {
            HashMap<String, Integer> samplerPercentile = new HashMap<String, Integer>(QUANTILES.length);

            AtomicLongArray samplerDistribution = samplerPercentileDistMap.get(sampler);
            long[] cumulativeShiftArray = samplerCumulativeShiftArrayMap.get(sampler);
            AtomicLong samplerCounter = samplerTotalCounterMap.get(sampler);

            if(cumulativeShiftArray == null) {
                cumulativeShiftArray = new long[timeout*1000 + 1];
                Arrays.fill(cumulativeShiftArray, zero);
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
            this.titleMap.put(sampler, (int)samplerThroughput);
            for(float f: QUANTILES) {
                samplerPercentile.put(String.valueOf(f * 100), (int)binarySearchMinIndex(samplerShiftArray, f));
            }
            result.put(sampler, samplerPercentile);
        }

        return result;
    }

    public HashMap<String, HashMap<String, Integer>> calculateCumulativeSamplerPercentile() {
        HashMap<String, HashMap<String, Integer>> result = new HashMap<String, HashMap<String, Integer>>(samplerPercentileDistMap.size());
        HashSet<String> samplers = new HashSet<String>();
        samplers.addAll(samplerPercentileDistMap.keySet());
        samplers.addAll(this.samplerCumulativeShiftArrayMap.keySet());
        for(String sampler : samplers) {
            HashMap<String, Integer> samplerCumulativePercentile = new HashMap<String, Integer>(QUANTILES.length);
            for(float f: QUANTILES) {
                samplerCumulativePercentile.put(String.valueOf(f * 100), (int)binarySearchMinIndex(this.samplerCumulativeShiftArrayMap.get(sampler), f));
            }
            result.put(sampler, samplerCumulativePercentile);
        }
        return result;
    }

    private int[] calculateSecondTotalIntervalDistribution() {
        long sum;
        int prev = 0;
        int[] distList = new int[TIME_PERIODS.length];
        for(int i=0; i<TIME_PERIODS.length;++i) {
            sum = 0;
            for(int j=prev; j< TIME_PERIODS[i] && j < this.percentileDistArray.length();++j) {
                sum += this.percentileDistArray.get(j); //SSE ?
            }
            distList[i] = (int)sum;
            prev = TIME_PERIODS[i];
        }
        return distList;
    }

    private HashMap<String, int[]> calculateSamplerSecondTotalIntervalDistribution() {
        HashMap<String, int[]> result = new HashMap<String, int[]>(samplerPercentileDistMap.size());
        for(String sampler : samplerPercentileDistMap.keySet()) {
            int[] distList = new int[TIME_PERIODS.length];
            long sum;
            int prev = 0;
            for(int i=0; i<TIME_PERIODS.length;++i) {
                sum = 0;
                for(int j=prev; j < TIME_PERIODS[i] && j < this.samplerPercentileDistMap.get(sampler).length();j++) {
                    sum += this.samplerPercentileDistMap.get(sampler).get(j);
                }
                distList[i] = (int)sum;
                prev = TIME_PERIODS[i];
            }
            result.put(sampler, distList);
        }
        return result;
    }

    private HashMap<String, Integer> calculateSamplerAvgRT() {
        HashMap<String, Integer> result = new HashMap<String, Integer>(titleMap.size());
        for(String sample : sumRTSamplerMap.keySet()) {
            result.put(sample, (int)(sumRTSamplerMap.get(sample).get() / titleMap.get(sample)));
        }
        return result;
    }

    protected AgSecond aggregate() {
        AgSecond result = new AgSecond();

        result.time = System.currentTimeMillis() / 1000;
        result.second = this.second;

        result.avg_lt = (int)(sumLT / throughput);
        result.active_threads = active_threads;

        result.responseCodes = new HashMap<String, Integer>(this.responseCodeMap.size());
        for(String key : this.responseCodeMap.keySet()) {
            result.responseCodes.put(key, this.responseCodeMap.get(key).get());
        }

        result.inbound = this.inbound;
        result.outbound = this.outbound;
        result.avg_request_size = this.outbound / throughput;
        result.avg_response_size = this.inbound / throughput;

        result.percentile = calculateSecondTotalPercentile();
        result.sampler_percentile = calculateSecondSamplerPercentile();

        result.cumulative_percentile = calculateCumulativeTotalPercentile();
        result.cumulative_sampler_percentile = calculateCumulativeSamplerPercentile();

        result.time_periods = TIME_PERIODS;

        result.interval_dist = calculateSecondTotalIntervalDistribution();
        result.sampler_interval_dist = calculateSamplerSecondTotalIntervalDistribution();

        result.samplers = new HashMap<String, Integer>(this.titleMap.size());
        for(String key : this.titleMap.keySet()) {
            result.samplers.put(key, this.titleMap.get(key));
        }

        result.throughput = this.throughput;
        result.sampler_avg_rt = calculateSamplerAvgRT();
        result.avg_rt = sumRT / throughput;

        return result;
    }

    @Override
    public void run() {
        if(check() == false) {
            return; //Data not prepared
        }

        AgSecond agSecond = aggregate();

        try {
            JSONObject jsonSecond = new JSONObject();

            jsonSecond.put("time", agSecond.time);
            jsonSecond.put("second", agSecond.second);

            jsonSecond.put("avg_lt", agSecond.avg_lt);
            jsonSecond.put("active_threads", agSecond.active_threads);

            jsonSecond.put("rc", agSecond.responseCodes);
            jsonSecond.put("samplers", agSecond.samplers);

            JSONObject jsonTraffic = new JSONObject();
            jsonTraffic.put("inbound", agSecond.inbound);
            jsonTraffic.put("outbound", agSecond.outbound);
            jsonTraffic.put("avg_response_size", agSecond.avg_response_size);
            jsonTraffic.put("avg_request_size", agSecond.avg_request_size);

            jsonSecond.put("traffic", jsonTraffic);

            if(QUANTILES != null) {
                jsonSecond.put("percentile", agSecond.percentile);
                jsonSecond.put("sampler_percentile", agSecond.sampler_percentile);
                jsonSecond.put("cumulative_percentile", agSecond.cumulative_percentile);
                jsonSecond.put("cumulative_sampler_percentile", agSecond.cumulative_sampler_percentile);
            }
            if(TIME_PERIODS != null) {
                ArrayList<JSONObject> distList = new ArrayList<JSONObject>(agSecond.time_periods.length);
                int prev = 0;
                for(int i = 0 ;i<agSecond.time_periods.length;++i) {
                    JSONObject jsonInterval = new JSONObject();
                    jsonInterval.put("from", prev);
                    jsonInterval.put("to", agSecond.time_periods[i]);
                    jsonInterval.put("count", agSecond.interval_dist[i]);
                    prev = agSecond.time_periods[i];
                    distList.add(jsonInterval);
                }
                jsonSecond.put("interval_dist", distList);

                JSONObject jsonSamplerIntervalDistribution = new JSONObject();
                for(String sampler : agSecond.sampler_interval_dist.keySet()) {
                    distList = new ArrayList<JSONObject>(agSecond.time_periods.length);
                    prev = 0;
                    int[] sampler_interval_dist = agSecond.sampler_interval_dist.get(sampler);
                    for(int i = 0 ;i<agSecond.time_periods.length;++i) {
                        JSONObject jsonInterval = new JSONObject();
                        jsonInterval.put("from", prev);
                        jsonInterval.put("to", agSecond.time_periods[i]);
                        jsonInterval.put("count", sampler_interval_dist[i]);
                        prev = agSecond.time_periods[i];
                        distList.add(jsonInterval);
                    }
                    jsonSamplerIntervalDistribution.put(sampler, distList);
                }
                jsonSecond.put("sampler_interval_dist", jsonSamplerIntervalDistribution);


            }
            jsonSecond.put("th", agSecond.throughput);
            jsonSecond.put("sampler_avg_rt", agSecond.sampler_avg_rt);
            jsonSecond.put("avg_rt", agSecond.avg_rt);

            writer.write((jsonSecond.toJSONString() + "\n" ).toCharArray());
            writer.flush();
        } catch(Exception e) {
            log.error("Runnable exception", e);
        } finally {
            delete();
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
