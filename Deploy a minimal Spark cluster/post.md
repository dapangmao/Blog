

###Requirements

Since Spark is rapidly evolving, I need to deploy and maintain a minimal Spark cluster for the purpose of testing and prototyping. A public cloud is the best fit for my current demand. 

1. Intranet speed
   
   The cluster should easily copy the data from one server to another. MapReduce always shuffles a large chunk of data  throughout the HDFS. It's best that the hard disk is SSD.

2. Elasticity and scalability

   Before scaling the cluster out to more machines, the cloud should have some elasticity to size up or size down. 

3. Locality of Hadoop

   Most importantly, the Hadoop cluster and the Spark cluster should have one-to-one mapping relationship like below. The computation and the storage should always be on the same machines. 

| Hadoop  | Cluster Manager |  Spark | MapReduce | 
|----------|:-------------:|------:|-------:|
| Name Node |  Master | Driver | Job Tracker | 
| Data Node |  Slave   | Executor | Task Tracker | 

###Choice of public cloud: 
I simply compare two cloud service provider: AWS and DigitalOcean. Both have nice Python-based monitoring tools([Boto](https://github.com/boto/boto) for AWS and [python-digitalocean](https://github.com/koalalorenzo/python-digitalocean) for DigitalOcean). 

1. From storage to computation

   Hadoop's S3 is a great storage to keep data and load it into the Spark/EC2 cluster. Or the Spark cluster on EC2 can directly read S3 bucket such as s3n://file (the speed is still acceptable). On DigitalOcean, I have to upload data from local to the cluster's HDFS. 

2. DevOps tools:
   * AWS: [spark-ec2.py](https://github.com/apache/spark/blob/master/ec2/spark_ec2.py)
      - With default setting after running it, you will get
         - 2 HDFSs: one persistent and one ephemeral
         - Spark 1.3 or any earlier version
         - Spark's stand-alone cluster manager
      - A minimal cluster with 1 master and 3 slaves will be consist of 4 m1.xlarge EC2 instances 
         - Pros: large memory with each node having 15 GB memory 
         - Cons: not SSD; expensive (cost $0.35 * 6 = $2.1 per hour)
      
   * DigitalOcean: https://digitalocean.mesosphere.com/
      - With default setting after running it, you will get 
         - HDFS
         - no Spark
         - Mesos
         - OpenVPN
      - A minimal cluster with 1 master and 3 slaves will be consist of 4 2GB/2CPUs droplets 
         - Pros: as low as $0.12 per hour; Mesos provide fine-grained control over the cluster(down to 0.1 CPU and 16MB memory); nice to have VPN to guarantee the security
         - Cons: small memory(each has 2GB memory); have to install Spark manually
          
###Add Spark to DigitalOcean cluster
Tom Faulhaber has [a quick bash script](http://www.infolace.com/blog/2015/02/27/create-an-ad-hoc-spark-cluster/) for deployment. To install Spark 1.3.0, I write it into a [fabfile](https://github.com/dapangmao/Blog/blob/master/Deploy%20a%20minimal%20Spark%20cluster/fabfile.py) for [Python's Fabric](http://www.fabfile.org/). 
Then all the deployment onto the DigitOcean is just one command line. 
```python
# 10.1.2.3 is the internal IP address of the master
fab -H 10.1.2.3 deploy_spark 
```
*The source codes above are available at my [Github](https://github.com/dapangmao/Blog/tree/master/Deploy%20a%20minimal%20Spark%20cluster)*
