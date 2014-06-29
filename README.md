#insight

Data engineering project for Insight Data Engineering Fellows Program.

##Cluster Setup
The project was tested on multiple AWS ec2 clusters. A 4 node cluster with 1 M1.large (master node) and 3 M1.medium instances is the minimal recommonded setup to process this dataset. Tested on Cloudera Manager 5.0.1 and 5.0.2. The remaining instructions assume at least 1 AWS instance is up and you have access to an ssh identity file. 

1. Create multiple AWS ec2 instances. e.g. 1 M1.large for name node, and 3 M1.medium for data nodes.
2. Generate a new .pem file during instance creation.
3. SSH into the master (name) node.
4. Install Cloudera Manager 5.0.2.
```
wget http://archive-primary.cloudera.com/cm5/installer/5.0.2/cloudera-manager-installer.bin
chmod +x cloudera-manager-installer.bin
sudo ./cloudera-manager-installer.bin
```
5. Follow the interactive installer.

##Build 
Java 

