Setting up a Spark Cluster in AWS
================================
Kindly follow the instructions for setting up a Spark cluster in AWS from here: https://github.com/amplab/spark-ec2

Running PragmaticPPCA 
======================

The first step is to clone this repo in the master node. After cloning, you can then build it by running the following command
```
mvn clean package
```

The next step is to run sPCA-spark on a small toy matrix. There is an example script located in `sPCA/spca-spark/spca-spark_example.sh`. First, you need to set the environment variable `SPARK_HOME` to the directory where Spark is downloaded and installed:
```
export SPARK_HOME=<path/to/spark/directory> (e.g., /usr/lib/spark-1.0.0)
```
You can then run the example through the following command:
```
sPCA/spca-spark/spca-spark_example.sh local
```
where `local` means that the Spark code will run on the local machine. If the examples runs correctly, you should see a message saying `Principal components computed successfully`. The output will be written in `sPCA/spca-spark/output/`.
The example involves a command similar to the following:
```
$SPARK_HOME/bin/spark-submit --class org.qcri.sparkpca.SparkPCA --master <master_url> --driver-java-options "-Di=<path/to/input/matrix> -Do=<path/to/outputfolder> -Drows=<number of rows> -Dcols=<number of columns> -Dpcs=<number of principal components> [-DerrSampleRate=<Error sampling rate>] [-DmaxIter=<max iterations>] [-DoutFmt=<output format>] [-DComputeProjectedMatrix=<0/1 (compute projected matrix or not)>]" target/sparkPCA-1.0.jar 
```
This command runs sPCA on top of Spark in the local machine with one worker thread. The following is a description of the command-line arguments of sPCA:
- `<master-url>: `The master URL for the cluster (e.g. spark://23.195.26.187:7077), it is set to `local[K]` for running Spark in the local mode with *K* threads (ideally, set *K* to the number of cores on your machine). If this argument is set to `local`, the applications runs locally on one worker thread (i.e., no parlellism at all).
-	`<path/to/input/matrix>:` File or directory that contains an input matrix in the SequenceFile Format `<IntWritable key, VectorWritable value>`.
-	`<path/to/outputfolder>:` The directory where the resulting principal components is written
-	`<number of rows>:` Number of rows for the input matrix 
-	`<number of columns>:` Number of columns for the input matrix 
-	`<number of principal components>:` Number of desired principal components 
-	`[<Error sampling rate>](optional):` The error sampling rate [0-1] that is used for computing the error, It can be set to 0.01 to compute the error for only a small sample of the matrix, this speeds up the computations significantly 
- `[<max iterations>] (optional):` Maximum number of iterations before terminating, the default is 3
- `[<output format>] (optional):` One of three supported output formats (DENSE/COO/LIL), the default is DENSE. See Section Output Format for more details.
- `[<0/1 (compute projected matrix or not)>] (optional)` :  0 or 1 value that specifies whether the user wants to project the input matrix on the principal components or not. 1 means that the projected matrix will be computed, and 0 means it will not be computed. The projected matrix is written in the output folder specified  by `-DOutput`
=======
# PragmaticPPCA
The source code for the paper PragmaticPPCA
>>>>>>> b5620665a68fd9f31109162cad51af7921fc3e89
