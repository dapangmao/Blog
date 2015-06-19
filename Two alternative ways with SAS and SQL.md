



###Two alternative ways with SAS/SQL

I really appreciate those wonderful comments on my SAS posts by the readers ([1](http://www.sasanalysis.com/2015/02/solve-top-n-questions-in-sassql_3.html), [2](http://www.sasanalysis.com/2012/05/top-10-tips-and-tricks-about-proc-sql.html), [3](http://www.sasanalysis.com/2011/01/top-10-most-powerful-functions-for-proc.html)). They gave me a lot of inspirations. 

Due to SAS or SQL’s inherent limitation, recently I feel difficult in deal with some extremely large SAS datasets (it means that I exhausted all possible traditional ways). Here I conclude two alternative solutions in those cases as the follow-up to the comments. 

1.	Direct Read
    - Use a scripting language such as Python to read SAS datasets directly
2.	Code Generator 
    - Use SAS or other scripting languages to generate SAS/SQL codes

The example used is still `sashelp.class`, which has 19 rows. The target variable is `weight`.


    *In SAS
    data class;
        set sashelp.class;
        row = _n_;
    run;

####Example 1: Find the median

#####SQL Query

[In the comment](http://www.sasanalysis.com/2012/05/top-10-tips-and-tricks-about-proc-sql.html), Anders SköllermoFebruary wrote 
> Hi! About 1. Calculate the median of a variable:

> If you look at the details in the SQL code for calculation the median, 
then you find that the intermediate file is of size N*N obs, 
where N is the number of obs in the SAS data set. 

> So this is OK for very small files. But for a file with 10000 obs,
you have an intermediate file of size 100 million obs. 
/ Br Anders 
Anders Sköllermo Ph.D., Reuma and Neuro Data Analyst 

The SQL query below is simple and pure that can be ported to any other platform. However, just like what Anders said, it is just too expensive. 


    *In SAS
    proc sql;
       select avg(weight) as Median
       from (select e.weight
           from class e, class d
           group by e.weight
           having sum(case when e.weight = d.weight then 1 else 0 end)
              >= abs(sum(sign(e.weight - d.weight)))
        );
    quit;

#####PROC UNIVARIATE

[In the comment](http://www.sasanalysis.com/2012/05/top-10-tips-and-tricks-about-proc-sql.html), Anonymous wrote:
>I noticed the same thing - we tried this on one of our 'smaller' datasets (~2.9 million records), and it took forever.

>Excellent solution, but maybe PROC UNIVARIATE will get you there faster on a large dataset.

Definately, PROC UNIVARIATE in the best solution in SAS to find the median, which utilizes SAS's built-in powers. 


######Direct Read

When the extreme cases come, say SAS cannot even open the entire datasets, we may have to use the streaming method to read the sas7bdat file line by line. The sas7bdat format has been decoded by [Java](kasper.eobjects.org/2011/06/sassyreader-open-source-reader-of-sas.html), [R](http://cran.r-project.org/web/packages/sas7bdat/index.html) and [Python](https://pypi.python.org/pypi/sas7bdat). Theoretically we don't need to have SAS to query a SAS dataset.

[Heap](https://en.wikipedia.org/wiki/Heap_(data_structure)) is an interesting data structure, which easily finds a min or a max. ream the values, we could build a max heap and a min heap to cut the incoming stream into half in Python. The algorithm looks like a heap sorting. The good news is that it only read one variable each time and thus saves a lot of space.


    #In Python
    import heapq
    from sas7bdat import SAS7BDAT
    class MedianStream(object):
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
    
        def show_median(self):
            if self.N == 0:
                raise IOError('please use the insert method first')
            elif self.N % 2 == 0:
                return (-self.first_half[0] + self.second_half[0]) / 2.0
            return -self.first_half[0]
        
    if __name__ == "__main__": 
        stream = MedianStream()
        with SAS7BDAT('class.sas7bdat') as infile:
            for i, line in enumerate(infile):
                if i == 0:
                    continue
                stream.insert(float(line[-1]))
        print stream.show_median()

    99.5


####Example 2: Find top K by groups

#####SQL Query
This query below is very expensive. We have a self-joining O(N^2) and a sorting O(NlogN), and the total time complexity is a terrible O(N^2 + Nlog(N)).




    * In SAS
    proc sql; 
        select a.sex, a.name, a.weight, (select count(distinct b.weight) 
                from class as b where b.weight >= a.weight and a.sex = b.sex ) as rank 
        from class as a
        where calculated rank <= 3
        order by sex, rank
    ;quit;

#####Code Generator

The overall thought is break-and-conquer. If we synthesize SAS codes from a scripting tool such as Python, we essentially get many small SAS codes segments. For example, the SQL code below is just about sorting. So the time comlexity is largely decreased to O(Nlog(N)).


    # In Python
    def create_sql(k, candidates):
        template = """
        proc sql outobs = {0};
        select *
        from {1}
        where sex = '{2}' 
        order by weight desc
        ;
        quit;"""
        for x in candidates:
            current = template.format(k, 'class', x)
            print current
    
    create_sql(3, ['M', 'F'])

    
        proc sql outobs = 3;
        select *
        from class
        where sex = 'M' 
        order by weight desc
        ;
        quit;
    
        proc sql outobs = 3;
        select *
        from class
        where sex = 'F' 
        order by weight desc
        ;
        quit;


#####Direct Read

This time we use the data structure of heap again. To find the k top rows for each group, we just need to prepare the min heaps with the k size for each group. With the smaller values popped out everytime, we finally get the top k values for each group. The optimized time complexity is O(Nlog(k))


    from sas7bdat import SAS7BDAT
    from heapq import heappush, heappop
    
    def get_top(k, sasfile):
        minheaps = [[], []]
        sexes = ['M', 'F']
        with SAS7BDAT(sasfile) as infile:
            for i, row in enumerate(infile):
                if i == 0:
                    continue
                sex, weight = row[1], row[-1]
                i = sexes.index(sex)
                current = minheaps[i]
                heappush(current, (weight, row))
                if len(current) > k:
                    heappop(current)
        for x in minheaps:
            for _, y in x:
                print y
                
    if __name__ == "__main__":
        get_top(3, 'class.sas7bdat')


    [u'Robert', u'M', 12.0, 64.8, 128.0]
    [u'Ronald', u'M', 15.0, 67.0, 133.0]
    [u'Philip', u'M', 16.0, 72.0, 150.0]
    [u'Carol', u'F', 14.0, 62.8, 102.5]
    [u'Mary', u'F', 15.0, 66.5, 112.0]
    [u'Janet', u'F', 15.0, 62.5, 112.5]


####Example 3: Find Moving Window Maxium

At the daily work, I always want to find three statistics for a moving window: mean, max, and min. 

In his [blog post](http://www.sas-programming.com/2015/05/fast-sql-moving-average-calculation.html), Liang Xie showed three advanced approaches to calculated the moving averages, including `PROC EXPAND`, `DATA STEP` and `PROC SQL`. Apparently `PROC EXPAND` is the winner throughout the comparison. As conclusion, self-joining is very expensive and always O(N^2) and we should avoid it as much as possible. 

The question to find the max or the min is somewhat different other than to find the mean, since for the mean only the mean is memorized, while for the max/min the locations of the past min/max should also be memorized. 

#####Code Genenrator 
The strategy is very straighforward: we choose three rows from the table sequentially and calculate the means. The time complexity is O(k*N). The generated SAS code is very lengthy, but the machine should feel very comfortable to read it. 


In addition, if we want to save the results, we could insert those maximums to an empty table. 



    # In Python
    def create_sql(k, N):
        template = """
        select max(weight)
        from class
        where row in ({0})
        ;"""
        SQL = ""
        for x in range(1, N - k + 2):
            current = map(str, range(x, x + 3))
            SQL += template.format(','.join(current))
        print "proc sql;" + SQL + "quit;"
        
    create_sql(3, 19)
        

    proc sql;
        select max(weight)
        from class
        where row in (1,2,3)
        ;
        select max(weight)
        from class
        where row in (2,3,4)
        ;
        select max(weight)
        from class
        where row in (3,4,5)
        ;
        select max(weight)
        from class
        where row in (4,5,6)
        ;
        select max(weight)
        from class
        where row in (5,6,7)
        ;
        select max(weight)
        from class
        where row in (6,7,8)
        ;
        select max(weight)
        from class
        where row in (7,8,9)
        ;
        select max(weight)
        from class
        where row in (8,9,10)
        ;
        select max(weight)
        from class
        where row in (9,10,11)
        ;
        select max(weight)
        from class
        where row in (10,11,12)
        ;
        select max(weight)
        from class
        where row in (11,12,13)
        ;
        select max(weight)
        from class
        where row in (12,13,14)
        ;
        select max(weight)
        from class
        where row in (13,14,15)
        ;
        select max(weight)
        from class
        where row in (14,15,16)
        ;
        select max(weight)
        from class
        where row in (15,16,17)
        ;
        select max(weight)
        from class
        where row in (16,17,18)
        ;
        select max(weight)
        from class
        where row in (17,18,19)
        ;quit;


####Direct Read
Again, if we want to further decrease the time complexity, say O(N), we have to use better data structure, such as queue. SAS doesn't have queue, so we may switch to Python. Actually it has two loops which adds up to O(2N). However, it is still better than any other methods. 


    # In Python
    from sas7bdat import SAS7BDAT
    from collections import deque
    
    def maxSlidingWindow(A, w):
        N = len(A)
        ans =[0] * (N - w + 1)
        myqueue = deque()
        for i in range(w):
            while myqueue and A[i] >= A[myqueue[-1]]:
                myqueue.pop()
            myqueue.append(i)
        for i in range(w, N):
            ans[i - w] = A[myqueue[0]]
            while myqueue and A[i] >= A[myqueue[-1]]:
                myqueue.pop()
            while myqueue and myqueue[0] <= i-w:
                myqueue.popleft()
            myqueue.append(i)
        ans[-1] = A[myqueue[0]]
        return ans
    
    if __name__ == "__main__":
        weights = []
        with SAS7BDAT('class.sas7bdat') as infile:
            for i, row in enumerate(infile):
                if i == 0:
                    continue
                weights.append(float(row[-1]))
    
        print maxSlidingWindow(weights, 3)

    [112.5, 102.5, 102.5, 102.5, 102.5, 112.5, 112.5, 112.5, 99.5, 99.5, 90.0, 112.0, 150.0, 150.0, 150.0, 133.0, 133.0]


###Conclusion

While data is expanding, we should consider three things using SAS/SQL -

- Time complexity: we don't want run data for weeks.
- Space complexity: we don't want the memory overflow. 
- Clean codes: the collegues should easily read and modify the codes.
