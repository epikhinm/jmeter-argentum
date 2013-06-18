###JMeter Argentum

This is simple and fast results aggregation module for Apache JMeter. It collects, aggregates and writes results for every second as json-object into file.

####User Interface
![ArgentumListener](http://schiz.me/images/ag/ag.png)

**Timeout** is a delay time between executing samplers and aggregating. Aggregation timeout must be greater than timeout or response time of samplers. If sampler executed longer than this timeout, it would be skipped at the aggregation phase.

**Output File** is a file to store json-objects.

And you can use distributions, based on **percentiles** or **intervals**

**Percentiles** distribution is easy for human understanding, but more difficult for processor. **Interval** distribution divides the time at intervals, with lowering accuracy, but increases the performance of the calculation. You can use both of them with per sample distributions:)

####JSON Overview
#####Without RT-distributions
```javascript
{   "active_threads":8, //Active threads of JMeter
    "traffic":{
        "avg_request_size":0, //Average sampler request size
        "avg_response_size":0, //Average sampler response size
        "outbound":0, //Sum of all samplers requests sizes at this second
        "inbound":0 //Sum of all samplers responses sizes at this second
    },
    "avg_lt":60, //Average Latency
    "sampler_avg_rt":{
        "OK200":551, //Average response time for every samples
        "ERROR_WO_RC":1130,
        "ERROR502":399,
        "ERROR503":552
    },
    "second":1371502833, //UNIX timestamp
    "avg_rt":637, //Average response time
    "samplers":{
        "OK200":267368, //Map describes all sampler names and contains a count of samples
        "ERROR_WO_RC":133682,
        "ERROR502":133683,
        "ERROR503":133686
    },
    "rc":{
        "500":133682, //Map describes all sampler response codes and containt a count of them
        "200":267368,
        "502":133683,
        "503":133686
    },
    "th":668419 //Throughput
}

```
####Percentile Distribution
With this distribution common json-object contains two sub-objects:
```javascript
...
    "percentile":{ //One distribution for all samples
        "25.0":363,
        "50.0":508,
        "75.0":829,
        "80.0":893,
        "90.0":1133,
        "95.0":1571,
        "98.0":1835,
        "99.0":1923,
        "100.0":2010
    }
...
```

And distributions for every sampler:
```javascript
    "sampler_percentile":{
        "ERROR503":{ //Distribution for sample with name "ERROR503"
            "25.0":329,
            "50.0":554,
            "75.0":777, //75th percentile
            "80.0":822,
            "90.0":911, //90th percentile
            "95.0":956,
            "98.0":983,
            "99.0":992,
            "100.0":1000 //Maximum response time of "ERROR503" sampler
        },
        "ERROR_WO_RC":{ //Distribution for sample with name "ERROR_WO_RC"
            "25.0":694,
            "50.0":1133},
            "75.0":1571,
            "80.0":1659,
            "90.0":1835,
            "95.0":1923,
            "98.0":1976,
            "99.0":1993,
            "100.0":2010
         },
         ... //other distributions
    }
```

#####Interval Distribution
JSON-object also may containt summary interval distribution

```javascript
    ...
    "interval_dist": [
        {"to":5,"count":0,"from":0},
        {"to":10,"count":0,"from":5},
        {"to":100,"count":0,"from":10},
        {"to":500,"count":359533,"from":100}, //360k response with response time between 100 and 500 ms
        {"to":1000,"count":286426,"from":500},
        {"to":1500,"count":42055,"from":1000},
        {"to":2000,"count":41508,"from":1500},
        {"to":2500,"count":910,"from":2000}
        ],
    ...
```
You can determine the accuracy of this distribution. Just sets more intervals in text field.

Distributions for every sampler:
```javascript
    ...
    "sampler_interval_dist": {
        "ERROR503":[ //Distribution for sampler with name "ERROR503"
            {"to":5,"count":0,"from":0},
            {"to":10,"count":0,"from":5},
            {"to":100,"count":0,"from":10},
            {"to":500,"count":326800,"from":100}, //326k responses between 100 and 500 ms
            {"to":1000,"count":261504,"from":500},
            {"to":1500,"count":38212,"from":1000},
            {"to":2000,"count":37645,"from":1500},
            {"to":2500,"count":804,"from":2000}
        ],
        "ERROR_WO_RC":[ //Distribution for sampler with name "ERROR_WO_RC"
            {"to":5,"count":0,"from":0},
            {"to":10,"count":0,"from":5},
            {"to":100,"count":0,"from":10},
            {"to":500,"count":326800,"from":100},
            {"to":1000,"count":261504,"from":500},
            {"to":1500,"count":38212,"from":1000},
            {"to":2000,"count":37645,"from":1500},
            {"to":2500,"count":804,"from":2000}
        ],
        "OK200":[ //Distribution for sampler with name "OK200"
            {"to":5,"count":0,"from":0},
            {"to":10,"count":0,"from":5},
            {"to":100,"count":0,"from":10},
            {"to":500,"count":326800,"from":100},
            {"to":1000,"count":261504,"from":500},
            {"to":1500,"count":38212,"from":1000},
            {"to":2000,"count":37645,"from":1500},
            {"to":2500,"count":804,"from":2000}
        ],
        ...
    },
    ...
```


####Performance

CPU: Intel(R) Core(TM) i7 CPU         930  @ 2.80GHz
JVM: Oracle Hotspot 1.7u21
Apache Jmeter: 2.10

![performance](https://docs.google.com/spreadsheet/oimg?key=0Au50JydZm7UjdGJTU1B4bzJUVjdHenNXWWgzRGliQXc&oid=1&zx=22s6egeljlcx)

[google doc table](https://docs.google.com/spreadsheet/ccc?key=0Au50JydZm7UjdGJTU1B4bzJUVjdHenNXWWgzRGliQXc&usp=sharing)