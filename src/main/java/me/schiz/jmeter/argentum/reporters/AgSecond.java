package me.schiz.jmeter.argentum.reporters;

import java.util.HashMap;

public class AgSecond {
    public long time;  //time when second aggregated;
    public long second; //aggregation second;
    public int avg_lt; //average latency
    public int active_threads;
    public HashMap<String, Integer> responseCodes;
    public HashMap<String, Integer> samplers;

    //traffic
    public long inbound;
    public long outbound;
    public long avg_response_size;
    public long avg_request_size;

    public long throughput;
    public HashMap<String, Integer> sampler_avg_rt;
    public long avg_rt;

    //percentile
    public HashMap<String, Integer> percentile;
    public HashMap<String, HashMap<String, Integer>> sampler_percentile;
    public HashMap<String, Integer> cumulative_percentile;
    public HashMap<String, HashMap<String, Integer>> cumulative_sampler_percentile;

    //interval distribution
    public int[] time_periods;
    public int[] interval_dist;
    public HashMap<String, int[]> sampler_interval_dist;
}
