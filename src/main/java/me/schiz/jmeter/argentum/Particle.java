package me.schiz.jmeter.argentum;

import org.apache.jmeter.samplers.SampleResult;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Particle
    implements Comparable<Particle>{
    public int rt; //response time in ms
    public char[] name; //case name
    public char[] rc; //response code

    public final static char[] RC_OK = "200".toCharArray();
    public final static char[] RC_ERROR = "500".toCharArray();

    public Particle() {
    }

    public Particle(String name, int rt, String rc) {
        this.name = name.toCharArray();
        this.rt = rt;
        this.rc = rc.toCharArray();
    }

    public Particle(SampleResult sr) {
        if(sr.getResponseCode().isEmpty()) {
            if (sr.isSuccessful()) rc = RC_OK;
            else rc = RC_ERROR;
        } else {
            rc = sr.getResponseCode().toCharArray();
        }
        name = sr.getSampleLabel().toCharArray();
        rt = (int)sr.getTime();
    }

    @Override
    public int compareTo(Particle o) {
        return (int) (this.rt - o.rt);
    }


    public static void main(String[] args) {
        int count = 1000000;


        for(int j=1000;j-->0;) {
            List<Particle> particles = new LinkedList<Particle>();

            long start = System.currentTimeMillis();
            for(int i=count;i-->0;) {
                particles.add(new Particle("photon", (int)(Math.random() * 100000), "200"));
            }
            System.out.println("Loaded " + count + " particels, time " + (System.currentTimeMillis() - start) + "ms");
            start = System.currentTimeMillis();
            Collections.sort(particles);
            System.out.println("Sorted " + count + " particels, time " + (System.currentTimeMillis() - start) + "ms\n\n");
        }
    }
}
