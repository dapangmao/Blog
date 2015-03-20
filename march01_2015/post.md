###On a single machine

```python
import heapq
class find_median(object):
    def __init__(self):
        self.first_half = [] # will be a max heap
        self.second_half = [] # will be a min heap, 1/2 chance has one more element
        self.N = 0

    def insert(self, x):
        heapq.heappush(self.first_half, -x)
        self.N += 1
        if len(self.second_half) == len(self.first_half):
            to_second, to_first = map(heapq.heappop, [self.first_half, self.second_half])
            heapq.heappush(self.second_half, -to_second)
            heapq.heappush(self.first_half, -to_first)
        else:
            to_second = heapq.heappop(self.first_half)
            heapq.heappush(self.second_half, -to_second)
        
    def median(self):
        if self.N == 0:
            raise IOError('please use insert method to input')
        if self.N % 2 == 0:
            return (-self.first_half[0] + self.second_half[0]) / 2.0
        return self.second_half[0]

if __name__ == '__main__':
    test = find_median()
    input = [3, 5, 6, 9, 8, 1, 100, 2, 11, 7, 1]
    for i, x in enumerate(input):
        test.insert(x)
        print test.median(), test.first_half, test.second_half
```


###On a Spark cluster
```python
from pyspark.streaming import *
ssc = StreamingContext(sc, 31)

lines = ssc.socketTextStream(host, int(port))
inputs = lines.flatMap(lambda line: line.split(" ")).map(int)
test = find_median()

def process(time, rdd):
    try:
        for x in rdd.collect():
            test.insert(x)
        print "========= %s =========" % str(time)
        print test.median()
    except:
        pass

inputs.foreachRDD(process)
ssc.start()
ssc.awaitTermination()

```

####Further design
#####Step 1:
```python
class find_median:
    def findMedianSortedstreamrrays(self, stream, rdd):
        length = len(stream) + len(rdd)
        if length % 2 == 0:
            return ( self.findKth(stream, 0, rdd, 0, length / 2) + \
                self.findKth(stream, 0, rdd, 0, length / 2 + 1) ) / 2.0
        else:
            return self.findKth(stream, 0, rdd, 0, length / 2 + 1)

    def findKth(self, stream, stream_start, rdd, rdd_start, k):
        if stream_start >= len(stream):
            return rdd[rdd_start + k - 1]
        if rdd_start >= len(rdd):
            return stream[stream_start + k - 1]
        if k == 1:
            return min(stream[stream_start], rdd[rdd_start])
        if stream_start + k/2 -1 < len(stream):
            stream_key = stream[stream_start + k/2 -1]
        else:
            stream_key = 9223372036854775807
        if rdd_start + k/2 -1 < len(rdd):
            rdd_key = rdd[rdd_start + k/2 -1]
        else:
            rdd_key = 9223372036854775807
        if stream_key < rdd_key:
            return self.findKth(stream, stream_start + k / 2, rdd, rdd_start, k - k/2)
        else:
            return self.findKth(stream, stream_start, rdd, rdd_start + k / 2, k - k/2)
```

#####Step 2
```python
def merge(self, rdd, m, stream, n):
    m -= 1
    n -= 1
    last = m + n + 1
    while m >= 0 and n >= 0:
        if rdd[m] >= stream[n]:
            rdd[last] = rdd[m]
            m -= 1
        else:
            rdd[last] = stream[n]
            n -= 1
        last -= 1
    if n >= 0:
        for i, x in enumerate(stream[:n+1]):
            rdd[i] = x
```