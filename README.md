Setting up a Spark Cluster in AWS
================================
Kindly follow the instructions for setting up a Spark cluster in AWS from here: https://github.com/amplab/spark-ec2

Running PragmaticPPCA 
======================

The first step is to clone this repo in the master node. After cloning, you can then build it by running the following command
```
mvn clean package
```
If mvn is not installed, you can install mvn by running the following commands:
```
wget http://mirror.olnevhost.net/pub/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
tar xvf apache-maven-3.0.5-bin.tar.gz
mv apache-maven-3.0.5  /usr/local/apache-maven
export M2_HOME=/usr/local/apache-maven
export M2=$M2_HOME/bin 
export PATH=$M2:$PATH
source ~/.bashrc
mvn -version
```

To avoid any problems due to connection interrupt, install tmux and run all the remaining commands in tmux session
```sudo yum install tmux
tmux //to create a new session
tmux attach //to attach to the current session
ctrl b+d //to detach from the session
```
This command runs sPCA on top of Spark in the local machine with one worker thread. The following is a description of the command-line arguments of sPCA:

```
~/spark/bin/spark-submit --class org.pragmaticppca.PragmaticPPCA --master <master_url> --conf spark.cores.max=64 --conf spark.driver.maxResultSize=0 --conf spark.network.timeout=4000s --driver-memory <mb> --executor-memory <mb> --driver-java-options "-Di=<path/to/input/matrix> -Do=<path/to/outputfolder> -Drows=<number of rows>  -Dcols=<number of columns> -Dpcs=<number of principal components> -DsynMissing=<sythetically create missing ids?> -DmissingPerc=<if so by what percent?> -DwriteMissingIDs=<save these ids to file?> -DloadMissingIDs=<load missing ids from file? synMissing is set to false if true> -DiMissing=<path to missing ids> -DmaxIter=<max iterations> -DloadSeed=<load random starting point from file> -DiSeed=<path to seed> -DwriteSeed=<write current random starting point to file> -DhandleMissing=<handle missing value? if false missing ids are imputed with zero values> -DsketchEnable=<sketch to get a warm initialization point?> -DwritePC=<write principal components to file?> -Dclusters=<number of paritions>" ./target/sparkPCA-1.0.jar
```

We can derive (k+1)th singular value to check true error and derive error for the stored principal components efficiently by running codes from this: https://github.com/DataMiningResearch/sSketch


=======


