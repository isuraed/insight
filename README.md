#Review Insight

Customer review data sets can be analyzed from various angles. The Stanford Network Analysis Project (http://snap.stanford.edu) has a very large data set of Amazon product reviews. 

The goal of this project was to build a scaleable data pipeline to ingest the review data and answer several specific queries on the whole data set. The rough data pipeline is

![alt tag](https://raw.githubusercontent.com/isuraed/insight/master/pipeline.jpg)

I completed this project during a 3 week period as an Insight Data Engineering Fellow in Palo Alto, CA.

##Cluster Setup
I primarily used an Amazon ec2 cluster running 7 m1.xlarge machines. The minimum recommended setup is 1 m1.large (master node) and 3 m1.medium instances. Tested on Cloudera Manager 5.0.1 and 5.0.2 with Ubuntu 12.04 LTS. The remaining instructions assume at least 1 AWS instance is up and you have access to an ssh identity file. 

1. Create multiple AWS ec2 instances. e.g. 1 M1.large for name node, and 3 M1.medium for data nodes.
2. Generate a new .pem file during instance creation.
3. SSH into the name node.
4. Install Cloudera Manager 5.0.2.
```
wget http://archive-primary.cloudera.com/cm5/installer/5.0.2/cloudera-manager-installer.bin
chmod +x cloudera-manager-installer.bin
sudo ./cloudera-manager-installer.bin
```
5. Follow the interactive installer. The default services and roles are fine.

##Run the Pipeline
1. SSH into the name node.
2. Download the code.
```
git clone https://github.com/isuraed/insight.git
cd insight
```
3. Start the pipeline. This script installs the required dependencies, builds the code, and runs the pipeline.
```
python scripts/run_pipeline.sh
```

##Web App Setup
1. Create an ec2 instance running Ubuntu 12.04.
2. SSH to the ec2 instance.
3. Install git
```
sudo apt-get update
sudo apt-get install git
```
4. Get the code
```
git clone https://github.com/isuraed/insight.git
```
5. Run the setup script. This will install the required dependencies, setup Apache, and start the flask app inside apache2.
```
bash insight/server/setup.sh
```
