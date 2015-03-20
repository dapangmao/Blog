###Why a minimal cluster
1. Testing:
   
2. Prototyping

###Requirements

I need a  cluster that lives short time and handles ad-hoc requests of data analysis, or more specificly, running Spark. I want it to be quickly created to load data to memory. And I don't want to keep the cluster perpetually. Therefore, a public cloud may be the best fit for my demand. 

1. Intranet speed
   
   The cluster should easily copy the data from one server to another. Hadoop always have a large chunk of data shuffling in the HDFS. The hard disk should be SSD.

2. Elasticity and scalability

   Before scaling the cluter out to more machines, the cloud should have some elasicity to size up or size down 

3. Locality of Hadoop

   Most importantly, the Hadoop cluster and the Spark cluter should have one-to-one mapping relationship. 

| Hadooop  | Cluster Manager |  Spark | MapReduce | 
|----------|:-------------:|------:|-------:|
| Name Node |  Master | Driver | Job Tracker | 
| Data Node |  Slave   | Executor | Task Tracker | 

###Choice of public cloud: 
I simply compare two cloud service provider: AWS and DigitalOcean. Both have Python-based monitoring tools([Boto](https://github.com/boto/boto) for AWS and [python-digitalocean](https://github.com/koalalorenzo/python-digitalocean) for DigitalOcean ) . 

1. From storage to computation


2. DevOps tools:
   * AWS: [spark-ec2.py](https://github.com/apache/spark/blob/master/ec2/spark_ec2.py)
      - With default setting after running it, you will get
         - 2 HDFSs: one persistent and one ephemeral
         - Spark 1.3 or any earlier version
         - Spark's stand-alone cluster manager
      - A minimal cluster with 1 master and 3 slaves will be consist of 4 m1.xlarge instances by default
         - Pros: large memory with each node having 15 GB memory 
         - Cons: not SSD; expensive (cost $0.35 * 6 = $2.1 per hour)
      
   * Digital Ocean: https://digitalocean.mesosphere.com/
      - With default setting after runnning it, you will get 
         - HDFS
         - no Spark
         - Mesos
         - VPN: openvpn plays a significant role to assure the security
         - Pros: 0.12 per hour
         - Cons: small memory(each as 2GB memory)
         
      
###Add Spark to DigitalOcean cluster


