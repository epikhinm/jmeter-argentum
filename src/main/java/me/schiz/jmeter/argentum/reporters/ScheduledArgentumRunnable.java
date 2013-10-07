package me.schiz.jmeter.argentum.reporters;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.json.simple.JSONObject;

import java.io.Writer;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    protected boolean is_empty;

    //for per thread-configuration cumulative rt-distributions
    protected boolean rebuild_cumulative;
    private static int last_active_threads = 0;

    protected ConcurrentHashMap<String, AtomicLongArray> samplerPercentileDistMap;
    protected ConcurrentHashMap<String, long[]> samplerCumulativeShiftArrayMap;
    protected HashMap<String, Integer> totalSamplerAvgRTMap;
    protected ReentrantReadWriteLock.WriteLock readLock;

    protected int timeout;
    private static int last_threads_count;
    protected static HashMap<String, Object> last_total_metrics = new HashMap<String, Object>();
    protected static long last_aggregated_second;

    static float[] QUANTILES = null;
    static int[] TIME_PERIODS = null;
    static float[] DEFAULT_QUANTILES = {0.25f, 0.5f, 0.75f, 0.8f, 0.9f, 0.95f, 0.98f, 0.99f, 1.0f};
    static int[] DEFAULT_TIME_PERIODS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 11000};

    public ScheduledArgentumRunnable(ArgentumListener listener, Writer writer, boolean rebuildCumulative) {
        this.listener = listener;
        this.writer = writer;
        this.timeout = this.listener.getTimeout();
        this.rebuild_cumulative = rebuildCumulative;
    }

    public boolean check() {
        long now = System.currentTimeMillis() / 1000;
        long checkSecond = (now  - timeout - 1);

        if(checkSecond == last_aggregated_second) return false;
        if(this.listener.secSet.contains(checkSecond)) {
            log.debug("time: " + checkSecond + " queue: " + this.listener.secSet.toString());
            this.listener.secSet.remove(Long.valueOf(checkSecond));
            this.is_empty = false;
            this.second = checkSecond;//this.listener.secSet.pollFirst();
            this.active_threads = this.listener.threadsMap.get(this.second);
            last_threads_count = this.active_threads;
            this.throughput = 0;
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
            this.totalSamplerAvgRTMap = new HashMap<String, Integer>();
            this.readLock = this.listener.rwLockMap.get(second).writeLock();

        } else {
            if(last_aggregated_second == 0) return false;
            // empty second
            log.debug("time: " + now + " is empty");
            this.is_empty = true;
            this.second = checkSecond;
            this.active_threads = last_active_threads;
            this.throughput = 0;
            this.responseCodeMap = null;
            this.titleMap = null;
            this.sumRTSamplerMap = null;
            this.sumRT = 0;
            this.sumLT = 0;
            this.inbound = 0;
            this.outbound = 0;
            this.percentileDistArray = null;
            this.percentileShiftArray = this.listener.percentileDistShiftArray;
            this.samplerPercentileDistMap = null;
            this.samplerCumulativeShiftArrayMap = this.listener.samplerCumulativePercentileShiftArray;
            this.totalSamplerAvgRTMap = new HashMap<String, Integer>();
            this.readLock = null;
        }
        return true;
    }

    public void delete() {
        this.listener.threadsMap.remove(this.second);
        this.listener.sumLTMap.remove(this.second);
        this.listener.responseCodeMap.remove(this.second);
        this.listener.sumInboundTraffic.remove(this.second);
        this.listener.sumOutboundTraffic.remove(this.second);
        this.listener.percentileDistMap.remove(this.second);
        this.listener.samplerPercentileDistMap.remove(this.second);
        this.listener.rwLockMap.remove(this.second);
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
        for(String sampler : samplerPercentileDistMap.keySet()) {
            HashMap<String, Integer> samplerPercentile = new HashMap<String, Integer>(QUANTILES.length);

            AtomicLongArray samplerDistribution = samplerPercentileDistMap.get(sampler);
            long[] cumulativeShiftArray = samplerCumulativeShiftArrayMap.get(sampler);

            if(cumulativeShiftArray == null) {
                cumulativeShiftArray = new long[timeout*1000 + 1];
                Arrays.fill(cumulativeShiftArray, 0);
                samplerCumulativeShiftArrayMap.put(sampler, cumulativeShiftArray);
            }

            long samplerThroughput = 0;
            long i_rCount;
            long[] samplerShiftArray = new long[timeout*1000 + 1];
            AtomicLong sumRTSamplerCounter = sumRTSamplerMap.get(sampler);
            if(sumRTSamplerCounter == null) {
                sumRTSamplerCounter = new AtomicLong(0);
                sumRTSamplerMap.put(sampler, sumRTSamplerCounter);
            }
            BigInteger sum = BigInteger.ZERO;
            long last_shift_rt = 0;
            for(int i = 0; i < samplerDistribution.length() ; ++i) {
                i_rCount = samplerDistribution.get(i);
                if(i_rCount > 0) {
                    for(int j = i; j < samplerShiftArray.length; ++j)  {
                        samplerShiftArray[j] += i_rCount; //What about SSE?:)
                    }
                    for(int j = i; j < cumulativeShiftArray.length; ++j) {
                        cumulativeShiftArray[j] += i_rCount;    //SSE ?
                    }
                    // sum += cur_rt * (cur_count - last_count)
                    sum = sum.add(BigInteger.valueOf(i * (cumulativeShiftArray[i] - last_shift_rt)));
                    last_shift_rt = cumulativeShiftArray[i];
                    samplerThroughput += i_rCount;
              //      samplerCounter.getAndAdd(i_rCount);
              //    Atomic ? Nafeihoa ?
                    sumRTSamplerMap.get(sampler) .addAndGet(i_rCount * i);
                }
            }
            this.titleMap.put(sampler, (int)samplerThroughput);
            this.totalSamplerAvgRTMap.put(sampler, Integer.valueOf(sum.divide(BigInteger.valueOf(last_shift_rt)).toString()));
            for(float f: QUANTILES) {
                samplerPercentile.put(String.valueOf(f * 100), (int)binarySearchMinIndex(samplerShiftArray, f));
            }
            result.put(sampler, samplerPercentile);
        }

        return result;
    }

    public HashMap<String, HashMap<String, Integer>> calculateCumulativeSamplerPercentile() {
        HashMap<String, HashMap<String, Integer>> result = new HashMap<String, HashMap<String, Integer>>(samplerPercentileDistMap.size());
        for(String sampler : this.samplerCumulativeShiftArrayMap.keySet()) {
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

    private HashMap<String, Integer> calculateTotalSamplerStdDevRT() {
        HashMap<String, Integer> result = new HashMap<String, Integer>(titleMap.size());
        for(String sample : sumRTSamplerMap.keySet()) {
            long[] total_sampler_shift_array = this.samplerCumulativeShiftArrayMap.get(sample);
            long sampler_avg_rt = Long.valueOf(this.totalSamplerAvgRTMap.get(sample));
            long quad;
            long last_shift_count = 0;
            BigInteger sum = BigInteger.ZERO;
            for(int i=0;i<total_sampler_shift_array.length;++i) {
                quad = (i - sampler_avg_rt) * (i - sampler_avg_rt);
                sum = sum.add(BigInteger.valueOf(((total_sampler_shift_array[i] - last_shift_count) * quad)));
                last_shift_count = total_sampler_shift_array[i];
            }
            double s = Math.sqrt(Double.parseDouble(sum.divide(BigInteger.valueOf(last_shift_count)).toString()));
            if(Math.abs((int)s - s) >= 0.5d)  result.put(sample, (int)s+1);
            else    result.put(sample, (int)s);
        }
        return result;
    }

    protected AgSecond aggregate() {
        AgSecond result = new AgSecond();

        result.time = System.currentTimeMillis() / 1000;
        result.second = this.second;
        last_aggregated_second = this.second;

        if(!is_empty) {
            this.readLock.lock();

            if(rebuild_cumulative) {
                if(last_active_threads == 0) last_active_threads = result.active_threads;
                if(last_active_threads != result.active_threads) {
                    last_active_threads = result.active_threads;
                    //clear cumulative data!
                    Arrays.fill(this.percentileShiftArray, 0L);
                    for(String sampler : this.samplerCumulativeShiftArrayMap.keySet()) {
                        Arrays.fill(this.samplerCumulativeShiftArrayMap.get(sampler), 0L);
                    }
                    log.warn("all cumulative data cleared!");
                }
            }
            result.responseCodes = new HashMap<String, Integer>(this.responseCodeMap.size());
            for(String key : this.responseCodeMap.keySet()) {
                result.responseCodes.put(key, this.responseCodeMap.get(key).get());
            }

            result.inbound = this.inbound;
            result.outbound = this.outbound;

            result.percentile = calculateSecondTotalPercentile();
            result.sampler_percentile = calculateSecondSamplerPercentile();

            result.cumulative_percentile = calculateCumulativeTotalPercentile();
            last_total_metrics.put("cumulative_percentile", new HashMap<String, Integer>(result.cumulative_percentile));
            result.cumulative_sampler_percentile = calculateCumulativeSamplerPercentile();
            last_total_metrics.put("cumulative_sampler_percentile", new HashMap<String, HashMap<String, Integer>>(result.cumulative_sampler_percentile));

            result.time_periods = TIME_PERIODS;

            result.interval_dist = calculateSecondTotalIntervalDistribution();
            result.sampler_interval_dist = calculateSamplerSecondTotalIntervalDistribution();

            result.samplers = new HashMap<String, Integer>(this.titleMap.size());
            for(String key : this.titleMap.keySet()) {
                result.samplers.put(key, this.titleMap.get(key));
            }

            result.throughput = this.throughput;
            result.sampler_avg_rt = calculateSamplerAvgRT();

            if(throughput != 0){
                result.avg_request_size = this.outbound / throughput;
                result.avg_response_size = this.inbound / throughput;
                result.avg_rt = sumRT / throughput;
            }

            result.total_sampler_avg_rt = totalSamplerAvgRTMap;
            last_total_metrics.put("total_sampler_avg_rt", new HashMap<String, Integer>(result.total_sampler_avg_rt));
            result.total_sampler_std_dev_rt = calculateTotalSamplerStdDevRT();
            last_total_metrics.put("total_sampler_std_dev_rt", new HashMap<String, Integer>(result.total_sampler_std_dev_rt));

            delete();
            this.readLock.unlock();

        } else {
            result.responseCodes = null;
            result.inbound = 0;
            result.outbound = 0;

            result.percentile = null;
            result.sampler_percentile = null;

            Object obj_cumulative_percentile = last_total_metrics.get("cumulative_percentile");
            if(obj_cumulative_percentile instanceof HashMap) {
                result.cumulative_percentile = ((HashMap<String, Integer>) obj_cumulative_percentile);
            } else result.cumulative_percentile = null;

            Object obj_cumulative_sampler_percentile = last_total_metrics.get("cumulative_sampler_percentile");
            if(obj_cumulative_sampler_percentile instanceof HashMap) {
                result.cumulative_sampler_percentile = ((HashMap<String, HashMap<String, Integer>>) obj_cumulative_sampler_percentile);
            } else result.cumulative_sampler_percentile = null;

            result.time_periods = TIME_PERIODS;

            result.interval_dist = null;
            result.sampler_interval_dist = null;

            result.samplers = null;

            result.throughput = 0;
            result.sampler_avg_rt = null;

            result.avg_request_size = 0;
            result.avg_response_size = 0;
            result.avg_rt = 0;

            Object obj_total_sampler_avg_rt = last_total_metrics.get("total_sampler_avg_rt");
            if(obj_total_sampler_avg_rt instanceof HashMap) {
               result.total_sampler_avg_rt = ((HashMap<String, Integer>) obj_total_sampler_avg_rt);
            } else result.total_sampler_avg_rt = null;

            Object obj_total_sampler_std_dev_rt = last_total_metrics.get("total_sampler_std_dev_rt");
            if(obj_total_sampler_std_dev_rt instanceof HashMap) {
                result.total_sampler_std_dev_rt = ((HashMap<String, Integer>) obj_total_sampler_std_dev_rt);
            } else result.total_sampler_std_dev_rt = null;
        }
        return result;
    }

    @Override
    public void run() {
        try {
            if(check() == false) {
                return; //Data not prepared
            }

            AgSecond agSecond = aggregate();

            JSONObject jsonSecond = new JSONObject();

            JSONObject last = new JSONObject();
            JSONObject total = new JSONObject();

            last.put("time", agSecond.time);
            last.put("second", agSecond.second);

            last.put("active_threads", agSecond.active_threads);

            if(agSecond.throughput > 0) {
                last.put("avg_lt", agSecond.avg_lt);
                last.put("rc", agSecond.responseCodes);
                last.put("samplers", agSecond.samplers);

                JSONObject jsonTraffic = new JSONObject();
                jsonTraffic.put("inbound", agSecond.inbound);
                jsonTraffic.put("outbound", agSecond.outbound);
                jsonTraffic.put("avg_response_size", agSecond.avg_response_size);
                jsonTraffic.put("avg_request_size", agSecond.avg_request_size);

                last.put("traffic", jsonTraffic);
            }

            if(QUANTILES != null) {
                if(agSecond.throughput > 0) {
                    last.put("overall_rt_quantile", agSecond.percentile);
                    last.put("sampler_rt_quantile", agSecond.sampler_percentile);
                }
                total.put("overall_rt_quantile", agSecond.cumulative_percentile);
                total.put("sampler_rt_quantile", agSecond.cumulative_sampler_percentile);
            }
            if(TIME_PERIODS != null) {
                ArrayList<JSONObject> distList = new ArrayList<JSONObject>(agSecond.time_periods.length);
                int prev = 0;
                if(agSecond.throughput > 0) {
                    for(int i = 0 ;i<agSecond.time_periods.length;++i) {
                        JSONObject jsonInterval = new JSONObject();
                        jsonInterval.put("from", prev);
                        jsonInterval.put("to", agSecond.time_periods[i]);
                        jsonInterval.put("count", agSecond.interval_dist[i]);
                        prev = agSecond.time_periods[i];
                        distList.add(jsonInterval);
                    }
                    last.put("overall_interval_dist", distList);
                }
                if(agSecond.throughput > 0) {
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
                    last.put("sampler_interval_dist", jsonSamplerIntervalDistribution);
                }
            }
            total.put("sampler_avg_rt", agSecond.total_sampler_avg_rt);
            total.put("sampler_std_dev_rt", agSecond.total_sampler_std_dev_rt);

            last.put("throughput", agSecond.throughput);
            if(agSecond.throughput > 0) {
                last.put("sampler_avg_rt", agSecond.sampler_avg_rt);
                last.put("overall_avg_rt", agSecond.avg_rt);
            }

            jsonSecond.put("last", last);
            jsonSecond.put("total", total);

            writer.write((jsonSecond.toJSONString() + "\n" ).toCharArray());
            writer.flush();
        } catch(Exception e) {
            log.error("Runnable exception", e);
        } finally {
            //delete();
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
