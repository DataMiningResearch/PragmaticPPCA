package org.pragmaticppca;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Level;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.Functions;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.storage.StorageLevel;
import org.pragmaticppca.FileFormat.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.thoughtworks.xstream.io.json.JsonWriter.Format;

import breeze.linalg.randomInt;
import scala.Tuple2;


public class PragmaticPPCA implements Serializable {

	private final static Logger log = LoggerFactory.getLogger(PragmaticPPCA.class);// getLogger(SparkPCA.class);

	static String dataset = "Untitled";
	static long startTime, endTime, totalTime;
	public static int nClusters = 4;
	public static Stat stat = new Stat();

	public static void main(String[] args) throws FileNotFoundException {
		org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
		org.apache.log4j.Logger.getLogger("akka").setLevel(Level.ERROR);

		// Parsing input arguments
		final String inputPath;
		final String outputPath;
		final int nRows;
		final int nCols;
		final int nPCs;
		double tolerance;
		int subsample = 0;
		int missingPerc = 10;
		boolean sketchEnable;
		boolean synMissing;
		boolean writeMissingIDs=false;
		boolean handleMissing=false;
		String inputPathMissing=null;
	    boolean loadMissingIDs=false;

		String inputPathSeed = null;
		boolean loadSeed = false;
		boolean writeSeed = false;
		
		boolean writePC = false;

		try {
			inputPath = System.getProperty("i");
			if (inputPath == null)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			printLogMessage("i");
			return;
		}
		try {
			outputPath = System.getProperty("o");
			if (outputPath == null)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			printLogMessage("o");
			return;
		}

		try {
			nRows = Integer.parseInt(System.getProperty("rows"));
		} catch (Exception e) {
			printLogMessage("rows");
			return;
		}

		try {
			nCols = Integer.parseInt(System.getProperty("cols"));
		} catch (Exception e) {
			printLogMessage("cols");
			return;
		}


		try {
			tolerance = Double.parseDouble(System.getProperty("tolerance"));
			
			if(tolerance<0) {				
				System.out.println("Tolerance cannt be negative setting it to 0.05");
				tolerance=0.05;
			}
			else System.out.println("Tolerance is set to "+tolerance);
			
		} catch (Exception e) {
			tolerance = -1.0;
			System.out.println("Iteration is not stopped by tolerance");
			
		}

		try {
			sketchEnable = Boolean.parseBoolean(System.getProperty("sketchEnable"));
			System.out.println("sketchEnable is set to" + sketchEnable);
			if(sketchEnable){
				try {
					subsample = Integer.parseInt(System.getProperty("subSample"));
					System.out.println("Subsample is set to" + subsample);
				} catch (Exception e) {
					subsample=10;
					System.out.println("Subsample is set to" + 10);
				}
			}
		} catch (Exception e) {
			sketchEnable=false;
			System.out.println("skeching is not done");
		}
		
		try {
			synMissing = Boolean.parseBoolean(System.getProperty("synMissing"));
			
			if(synMissing){
				try {
					missingPerc = Integer.parseInt(System.getProperty("missingPerc"));
					System.out.println("missingPerc is set to" + missingPerc);
				} catch (Exception e) {
					System.out.println("missingPerc is set to 10");
				}
				
				try {
					writeMissingIDs = Boolean.parseBoolean(System.getProperty("writeMissingIDs"));
					System.out.println("writeMissingIDs is set to" + writeMissingIDs);
				} catch (Exception e) {
					System.out.println("writeMissingIDs is set to False");
				}
				
			}
			System.out.println("SynMiss is set to" + synMissing);
		} catch (Exception e) {
			synMissing=false;
			System.out.println("SynMiss is set to False");
		}
		
		try {
			loadMissingIDs = Boolean.parseBoolean(System.getProperty("loadMissingIDs"));
			if (loadMissingIDs){
				try {
					inputPathMissing = System.getProperty("iMissing");
					if (inputPathMissing == null)
						throw new IllegalArgumentException();
				} catch (Exception e) {
					printLogMessage("iMissing");
					return;
				}
			}
			System.out.println("loadMissingIDs is set to:"+loadMissingIDs);	
		} catch (Exception e) {
			System.out.println("loadMissingIDs is set to"+loadMissingIDs);	
			
		}
		
		if(synMissing || loadMissingIDs){
			try {
				handleMissing = Boolean.parseBoolean(System.getProperty("handleMissing"));
				System.out.println("HandleMissing is set to" + handleMissing);
			} catch (Exception e) {
				System.out.println("handleMissing is set to False");
			}
		}
		
	
		try {
			writePC  = Boolean.parseBoolean(System.getProperty("writePC"));
			System.out.println("writePC is set to" + writePC);
		} catch (Exception e) {
			System.out.println("writePC is set to False");
		}
		
		try {
			loadSeed = Boolean.parseBoolean(System.getProperty("loadSeed"));
			if (loadSeed){
				try {
					inputPathSeed = System.getProperty("iSeed");
					if (inputPathSeed == null)
						throw new IllegalArgumentException();
				} catch (Exception e) {
					printLogMessage("iSeed");
					return;
				}
			}
			System.out.println("loadSeed is set to:"+ loadSeed);	
		} catch (Exception e) {
			System.out.println("loadSeed is set to false");				
		}
		
		try {
			writeSeed = Boolean.parseBoolean(System.getProperty("writeSeed"));
			System.out.println("writeSeed is set to true");	
		} catch (Exception e) {
			System.out.println("writeSeed is set to false");				
		}
		
		try {

			if (Integer.parseInt(System.getProperty("pcs")) == nCols) {
				nPCs = nCols - 1;
				System.out
						.println("Number of princpal components cannot be equal to number of dimension, reducing by 1");
			} else
				nPCs = Integer.parseInt(System.getProperty("pcs"));
		} catch (Exception e) {
			printLogMessage("pcs");
			return;
		}
		/**
		 * Defaults for optional arguments
		 */
		int maxIterations = 50;
	

		try {
			nClusters = Integer.parseInt(System.getProperty("clusters"));
			System.out.println("No of partition is set to" + nClusters);
		} catch (Exception e) {
			log.warn("Cluster size is set to default: " + nClusters);
		}

		try {
			maxIterations = Integer.parseInt(System.getProperty("maxIter"));
		} catch (Exception e) {
			log.warn("maximum iterations is set to default: maximum	Iterations=" + maxIterations);
		}

		try {
			dataset = System.getProperty("dataset");
		} catch (IllegalArgumentException e) {
			log.warn("Invalid Format " + System.getProperty("outFmt") + ", Default name for dataset" + dataset
					+ " will be used ");
		} catch (Exception e) {
			log.warn("Default oname for dataset " + dataset + " will be used ");
		}
		

		// Setting Spark configuration parameters
		SparkConf conf = new SparkConf().setAppName("pragmaticPPCA");//.setMaster("local[*]");//
		// TODO
		// remove
		// this
		// part
		// for
		// building
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryoserializer.buffer.max", "128m");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// compute principal components
		computePrincipalComponents(sc, inputPath, outputPath, nRows, nCols, nPCs, subsample, maxIterations, 
				tolerance, synMissing, handleMissing, writeMissingIDs, missingPerc, sketchEnable, 
				inputPathMissing, loadMissingIDs,
				loadSeed, inputPathSeed, writeSeed, writePC);

		// log.info("Principal components computed successfully ");
	}

	public static Map synMissing(JavaSparkContext sc,JavaPairRDD<IntWritable, VectorWritable> seqVectors,
			final int nRows, final int nCols, double percent){

		/**
		 *  Synthetically create missing data
		 *  Actually create missing data by randomly selecting non zero ids
		 *  In reality, we have to have ids of missing datapoints
		 */
		
		//first determine how many non zero elements there are
		final Accumulator<Integer> nnz = sc.accumulator(new Integer(0));
		//how many nonzero elements does each row possess
		final Accumulator<double[]> noCol = sc.accumulator(new double[nRows], new VectorAccumulatorParam());
		//TODO check if the following distributed code works 
		JavaRDD<org.apache.spark.mllib.linalg.Vector> calnnz = seqVectors
				.map(new Function<Tuple2<IntWritable, VectorWritable>, org.apache.spark.mllib.linalg.Vector>() {

					public org.apache.spark.mllib.linalg.Vector call(Tuple2<IntWritable, VectorWritable> arg0)
							throws Exception {

						org.apache.mahout.math.Vector mahoutVector = arg0._2.get();
						nnz.add(mahoutVector.getNumNonZeroElements());
						noCol.localValue()[arg0._1.get()]=mahoutVector.getNumNonZeroElements();
						return null;
					}
				});
		//no map action with out reduce operation
		calnnz.count();
		int nnzElems=nnz.value();
		double[] n=noCol.value();
		int size=(int) (nnzElems*percent/100);
		Random r = new Random();
		int count=0;
		//randomly choose a row, then randomly choose the serial of a nonzero element
		//using map for less memory, and ease of search
		Map<Integer, ArrayList<Integer>> id=new HashedMap();
		while(true){
			int idx = r.nextInt(nRows);
			int idy = r.nextInt((int)n[idx]); 
			if(id.get(idx)!=null){
				if(id.get(idx).contains(idy)){
					continue;
				}
				else{
					id.get(idx).add(idy);
					count=count+1;
				}
			}
			else{
				ArrayList<Integer> colEntry=new ArrayList<Integer>();
				colEntry.add(idy);
				id.put(new Integer(idx), colEntry);
				count=count+1;				
			}
			if(count==size) break;
		}
		
		
		return id;
		
		
	}
	/**
	 * Compute principal component analysis where the input is a path for a
	 * hadoop sequence File <IntWritable key, VectorWritable value>
	 * 
	 * @param sc
	 *            Spark context that contains the configuration parameters and
	 *            represents connection to the cluster (used to create RDDs,
	 *            accumulators and broadcast variables on that cluster)
	 * @param inputPath
	 *            Path to the sequence file that represents the input matrix
	 * @param nRows
	 *            Number of rows in input Matrix
	 * @param nCols
	 *            Number of columns in input Matrix
	 * @param nPCs
	 *            Number of desired principal components
	 * @param errRate
	 *            The sampling rate that is used for computing the
	 *            reconstruction error
	 * @param maxIterations
	 *            Maximum number of iterations before terminating
	 * @param sketchEnable 
	 * @param inputPathMissing 
	 * @param loadMissingIDs 
	 * @param inputPathSeed 
	 * @param writeSeed 
	 * @param loadSeed 
	 * @return Matrix of size nCols X nPCs having the desired principal
	 *         components
	 * @throws FileNotFoundException
	 */
	public static org.apache.spark.mllib.linalg.Matrix computePrincipalComponents(JavaSparkContext sc, String inputPath,
			String outputPath, final int nRows, final int nCols, final int nPCs, 
			final int subsample,  int maxIterations,  double tolerance, 
			boolean synMissing, boolean handleMissing, boolean writeMissingIDs, final int missingPerc, 
			boolean sketchEnable, String inputPathMissing, boolean loadMissingIDs,
			boolean loadSeed, String inputPathSeed, boolean writeSeed, boolean writePC) throws FileNotFoundException {

		/**
		 * preprocess the data
		 * 
		 * @param nClusters
		 * 
		 */
		startTime = System.currentTimeMillis();

		// Read from sequence file
		JavaPairRDD<IntWritable, VectorWritable> seqVectors = sc.sequenceFile(inputPath, IntWritable.class,
				VectorWritable.class, nClusters);
		//This is all we need for missing data, an map of missing indices
		
		
		final Broadcast<Map<Integer, ArrayList<Integer>>> missingIDs;
		
		if(synMissing){
			Map<Integer, ArrayList<Integer>> id=synMissing(sc,seqVectors, nRows,nCols,missingPerc);
			if(writeMissingIDs){
				try {
			         FileOutputStream fileOut =
			         new FileOutputStream(outputPath+File.separator+"mapMissingIDs.ser");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(id);
			         out.close();
			         fileOut.close();
			         System.out.printf("Serialized data is saved in the output path");
			      } catch (IOException i) {
			         i.printStackTrace();
			      }
			}
			missingIDs=sc.broadcast(id);
		}
		else if(loadMissingIDs){
			Map<Integer, ArrayList<Integer>> id =null;
			try {
		         FileInputStream fileIn = new FileInputStream(inputPathMissing+File.separator+"mapMissingIDs.ser");
		         ObjectInputStream in = new ObjectInputStream(fileIn);
		         id = (Map<Integer, ArrayList<Integer>>) in.readObject();
		         in.close();
		         fileIn.close();
		      } catch (IOException i) {
		         i.printStackTrace();
		         return null;
		      } catch (ClassNotFoundException c) {
		         System.out.println("Missing ID class not found");
		         c.printStackTrace();
		         return null;
		      }
			missingIDs=sc.broadcast(id);
		}
		else{
			missingIDs=sc.broadcast(null);
		}
		
		final Accumulator<Double> check = sc.accumulator(new Double(0));
		
		JavaPairRDD<org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector> vectors 
		
		=seqVectors.mapToPair(new PairFunction<Tuple2<IntWritable, VectorWritable>,
						org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector>(){

				@Override
				public Tuple2<org.apache.spark.mllib.linalg.Vector, org.apache.spark.mllib.linalg.Vector> call(
						Tuple2<IntWritable, VectorWritable> t) throws Exception {
					// TODO Auto-generated method stub
					Vector mahoutVector = t._2.get();
					Iterator<Element> elements = mahoutVector.nonZeroes().iterator();
					ArrayList<Tuple2<Integer, Double>> tupleListObs = new ArrayList<Tuple2<Integer, Double>>();
					ArrayList<Tuple2<Integer, Double>> tupleListMiss = new ArrayList<Tuple2<Integer, Double>>();
					ArrayList<Integer> missingIdx = null;
					
					if(missingIDs.value()!=null){
						if(missingIDs.getValue().containsKey(t._1.get()))
							missingIdx = missingIDs.getValue().get(t._1.get());
					}
					
					
					int i=0;
					while (elements.hasNext()) {
						Element e = elements.next();
						if (e.index() >= nCols || e.get() == 0)
							continue;
						
						if(missingIdx!=null){//checking if the column is here
							if(missingIdx.contains(e.index())){
								Tuple2<Integer, Double> tuple =
										new Tuple2<Integer, Double>(e.index(), new Double(0));
								tupleListMiss.add(tuple);
								check.add(1.0);
								continue;							
							}
						}

						Tuple2<Integer, Double> tuple =
						new Tuple2<Integer, Double>(e.index(), e.get());
						tupleListObs.add(tuple);
						
						
						
					}
					org.apache.spark.mllib.linalg.Vector obsVector = Vectors.sparse(nCols, tupleListObs);
					org.apache.spark.mllib.linalg.Vector missVector = Vectors.sparse(nCols, tupleListMiss);
					
					return new Tuple2<org.apache.spark.mllib.linalg.Vector,
							org.apache.spark.mllib.linalg.Vector>(obsVector,missVector);		
				}
				
			}).persist(StorageLevel.MEMORY_ONLY_SER());
		
	
		//vectors.count();
		System.out.println(check.value());
		
		/**
		 * get the missing indices for //TODO debug purpose only
		 *
		
		if(writeMissingIDs){
			List list=vectors.collect();
			Iterator iter=list.iterator();
			int[][] ids=new int[nRows][nCols];
			int k=0;
			while(iter.hasNext()){
				Tuple2 tup=(Tuple2)iter.next();
				org.apache.spark.mllib.linalg.Vector e=(org.apache.spark.mllib.linalg.Vector)tup._2;
				int[] indices = ((SparseVector) e).indices();
				int i;
				// iterate over non
				for (i = 0; i < indices.length; i++) {
					ids[k][indices[i]]=1;
				}
				k++;
			}
				
			try {
				
				String fileLocation = outputPath+ File.separator +"missingIDs.txt"; 
				
				
				
				PrintWriter out = null;
				
				out = new PrintWriter(fileLocation);
				    
				
				
				for(int i=0;i<nRows;i++){
					for(int j=0;j<nCols;j++){

						out.print(ids[i][j]+" ");				
					}
					out.println();
				}
				
			    out.close();
				
			}
			catch (Exception e) {
				System.out.println("NO");
				Log.error("Output file " + outputPath + " not found ");
			}	
		}
		
		
		/*
		 * end of getting missing ids //TODO debug purpose only
		 */
			
		// 1. Mean Job : This job calculates the mean and span of the columns of
				// the input RDD<org.apache.spark.mllib.linalg.Vector>
		final Accumulator<double[]> matrixAccumY = sc.accumulator(new double[nCols], new VectorAccumulatorParam());
		final double[] internalSumY = new double[nCols];
		vectors.foreachPartition(new VoidFunction<Iterator<
				Tuple2<org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector>>>() {

			public void call(Iterator<Tuple2<org.apache.spark.mllib.linalg.Vector,
					org.apache.spark.mllib.linalg.Vector>> arg0) throws Exception {
				org.apache.spark.mllib.linalg.Vector yObsi;
				org.apache.spark.mllib.linalg.Vector yMissi;
				int[] indicesObs = null;
				int[] indicesMiss = null;
				int i;
				while (arg0.hasNext()) {
					Tuple2 <org.apache.spark.mllib.linalg.Vector,
					org.apache.spark.mllib.linalg.Vector> entry= arg0.next();
					yObsi = entry._1;
					yMissi= entry._2;
					indicesObs = ((SparseVector) yObsi).indices();
					indicesMiss = ((SparseVector) yMissi).indices();
					for (i = 0; i < indicesObs.length; i++) {
						internalSumY[indicesObs[i]] += yObsi.apply(indicesObs[i]);
					}
					for (i = 0; i < indicesMiss.length; i++) {
						internalSumY[indicesMiss[i]] += yMissi.apply(indicesMiss[i]);
					}
				}
				matrixAccumY.add(internalSumY);
			}

		});// End Mean Job
		
		System.out.println("Number of missing elements"+check.value());
		
		Vector meanVector = new DenseVector(matrixAccumY.value()).divide(nRows);
		final Broadcast<Vector> br_ym_mahout = sc.broadcast(meanVector);
		double meanSquareSumTmp = 0;
		for (int i = 0; i < br_ym_mahout.value().size(); i++) {
			double element = br_ym_mahout.value().getQuick(i);
			meanSquareSumTmp += element * element;
		}
		final double meanSquareSum = meanSquareSumTmp;
		final Accumulator<Double> doubleAccumNorm2 = sc.accumulator(0.0);
		vectors.foreach(new VoidFunction<
				Tuple2<org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector>>() {
			
			public void call(Tuple2<org.apache.spark.mllib.linalg.Vector,
					org.apache.spark.mllib.linalg.Vector> arg0) throws Exception {
				
				double norm2 = 0;
				double meanSquareSumOfZeroElements = meanSquareSum;
				int[] indicesObs = ((SparseVector) arg0._1).indices();
				int[] indicesMiss = ((SparseVector) arg0._2).indices();
				int i;
				int index;
				double v;
				// iterate over non
				for (i = 0; i < indicesObs.length; i++) {
					index = indicesObs[i];
					v = arg0._1.apply(index);
					double mean = br_ym_mahout.value().getQuick(index);
					double diff = v - mean;
					diff *= diff;

					// cancel the effect of the non-zero element in
					// meanSquareSum
					meanSquareSumOfZeroElements -= mean * mean;
					norm2 += diff;
				}
				for (i = 0; i < indicesMiss.length; i++) {
					index = indicesMiss[i];
					v = arg0._2.apply(index);
					double mean = br_ym_mahout.value().getQuick(index);
					double diff = v - mean;
					diff *= diff;

					// cancel the effect of the non-zero element in
					// meanSquareSum
					meanSquareSumOfZeroElements -= mean * mean;
					norm2 += diff;
				}
				// For all all zero items, the following has the sum of mean
				// square
				norm2 += meanSquareSumOfZeroElements;
				doubleAccumNorm2.add(norm2);
			}

		});// end Frobenious Norm Job

		double norm2 = doubleAccumNorm2.value();

		endTime = System.currentTimeMillis();
		totalTime = endTime - startTime;

		stat.preprocessTime = (double) totalTime / 1000.0;

		stat.totalRunTime = stat.preprocessTime;

		stat.appName = "pragmaticPPCA";
		stat.dataSet = dataset;
		stat.nRows = nRows;
		stat.nCols = nCols;
		
		boolean repeat=false;
		Scanner scanner = new Scanner(System.in);
		do{
			System.out.println("Compute Principal components with current settings? True or False: ");
			boolean noChange= scanner.nextBoolean();
			if(!noChange){
				System.out.println("Tolerance?: ");
				tolerance=scanner.nextDouble();
				System.out.println("MaxIterations: ");
				maxIterations=scanner.nextInt();
				System.out.println("SketchEnable: ");
				sketchEnable=scanner.nextBoolean();
				System.out.println("HandleMissing: ");
				handleMissing=scanner.nextBoolean();
				System.out.println("LoadSeed: ");
				loadSeed=scanner.nextBoolean();
				System.out.println("WriteSeed: ");
				writeSeed=scanner.nextBoolean();
				System.out.println("WritePC: ");
				writePC=scanner.nextBoolean();
				
			}

			// compute principal components
			computePrincipalComponents(sc, vectors, br_ym_mahout, meanVector, norm2, outputPath, nRows, nCols, nPCs,
					subsample, tolerance,  maxIterations, handleMissing, sketchEnable,loadSeed, inputPathSeed, writeSeed, writePC);
		
			System.out.println("Do you want to continue?: True or False");
			repeat= scanner.nextBoolean();
			
		}while(repeat);
		
		scanner.close();

		
		
		
		
		
		
		System.out.println(meanVector);
		
		// Get the sum of column Vector from the accumulator and divide each
		// element by the number of rows to get the mean
		// not best of practice to use non-final variable
		
		// count the average ppca runtime

		

		return null;
	}
	
	public static org.apache.mahout.math.SingularValueDecomposition sketch(JavaSparkContext sc,
			JavaPairRDD<org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector> vectors,
			final Broadcast<Vector> br_ym_mahoutObs, final int nRows, final int nCols, final int nPCs,
			final int subsample, final Matrix Seed){

		final int s=nPCs+subsample;
		// Broadcast Y2X because it will be used in several jobs and several
		// iterations.
		final Broadcast<Matrix> br_Seed = sc.broadcast(Seed);

		// Xm = Ym * Y2X
		Vector zm_mahout = new DenseVector(s);
		zm_mahout = PCAUtils.denseVectorTimesMatrix(br_ym_mahoutObs.value(), Seed, zm_mahout);

		// Broadcast Xm because it will be used in several iterations.
		final Broadcast<Vector> br_zm_mahout = sc.broadcast(zm_mahout);
		// We skip computing X as we generate it on demand using Y and Y2X

		// 3. X'X and Y'X Job: The job computes the two matrices X'X and Y'X
		/**
		 * Xc = Yc * MEM (MEM is the in-memory broadcasted matrix Y2X)
		 * 
		 * XtX = Xc' * Xc
		 * 
		 * YtX = Yc' * Xc
		 * 
		 * It also considers that Y is sparse and receives the mean vectors Ym
		 * and Xm separately.
		 * 
		 * Yc = Y - Ym
		 * 
		 * Xc = X - Xm
		 * 
		 * Xc = (Y - Ym) * MEM = Y * MEM - Ym * MEM = X - Xm
		 * 
		 * XtX = (X - Xm)' * (X - Xm)
		 * 
		 * YtX = (Y - Ym)' * (X - Xm)
		 * 
		 */
		final Accumulator<double[][]> matrixAccumZtZ = sc.accumulator(new double[s][s],
				new MatrixAccumulatorParam());
		final Accumulator<double[][]> matrixAccumYtZ = sc.accumulator(new double[nCols][s],
				new MatrixAccumulatorParam());
		final Accumulator<double[]> matrixAccumZ = sc.accumulator(new double[s], new VectorAccumulatorParam());

		/*
		 * Initialize the output matrices and vectors once in order to avoid
		 * generating massive intermediate data in the workers
		 */
		final double[][] resArrayYtZ = new double[nCols][s];
		final double[][] resArrayZtZ = new double[s][s];
		final double[] resArrayZ = new double[s];

		/*
		 * Used to sum the vectors in one partition.
		 */
		final double[][] internalSumYtZ = new double[nCols][s];
		final double[][] internalSumZtZ = new double[s][s];
		final double[] internalSumZ = new double[s];

		vectors.foreachPartition(new VoidFunction<Iterator<
				Tuple2<org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector>>>() {

			public void call(Iterator<Tuple2<org.apache.spark.mllib.linalg.Vector,
					org.apache.spark.mllib.linalg.Vector>> arg0) throws Exception {
				org.apache.spark.mllib.linalg.Vector yi;
				while (arg0.hasNext()) {
					yi = arg0.next()._1;

					/*
					 * Perform in-memory matrix multiplication xi = yi' * Y2X
					 */
					PCAUtils.sparseVectorTimesMatrix(yi, br_Seed.value(), resArrayZ);

					// get only the sparse indices
					int[] indices = ((SparseVector) yi).indices();

					PCAUtils.outerProductWithIndices(yi, br_ym_mahoutObs.value(), resArrayZ, br_zm_mahout.value(),
							resArrayYtZ, indices);
					PCAUtils.outerProductArrayInput(resArrayZ, br_zm_mahout.value(), resArrayZ, br_zm_mahout.value(),
							resArrayZtZ);
					int i, j, rowIndexYtZ;

					// add the sparse indices only
					for (i = 0; i < indices.length; i++) {
						rowIndexYtZ = indices[i];
						for (j = 0; j < s; j++) {
							internalSumYtZ[rowIndexYtZ][j] += resArrayYtZ[rowIndexYtZ][j];
							resArrayYtZ[rowIndexYtZ][j] = 0; // reset it
						}

					}
					for (i = 0; i < s; i++) {
						internalSumZ[i] += resArrayZ[i];
						for (j = 0; j < s; j++) {
							internalSumZtZ[i][j] += resArrayZtZ[i][j];
							resArrayZtZ[i][j] = 0; // reset it
						}

					}
				}
				matrixAccumZ.add(internalSumZ);
				matrixAccumZtZ.add(internalSumZtZ);
				matrixAccumYtZ.add(internalSumYtZ);
			}

		});// end X'X and Y'X Job

		/*
		 * Get the values of the accumulators.
		 */
		Matrix centralYtZ = new DenseMatrix(matrixAccumYtZ.value());
		Matrix centralZtZ = new DenseMatrix(matrixAccumZtZ.value());
		Vector centralSumZ = new DenseVector(matrixAccumZ.value());

		/*
		 * Mi = (Yi-Ym)' x (Xi-Xm) = Yi' x (Xi-Xm) - Ym' x (Xi-Xm)
		 * 
		 * M = Sum(Mi) = Sum(Yi' x (Xi-Xm)) - Ym' x (Sum(Xi)-N*Xm)
		 * 
		 * The first part is done in the previous job and the second in the
		 * following method
		 */
		centralYtZ = PCAUtils.updateXtXAndYtx(centralYtZ, centralSumZ, br_ym_mahoutObs.value(), zm_mahout, nRows);
		centralZtZ = PCAUtils.updateXtXAndYtx(centralZtZ, centralSumZ, zm_mahout, zm_mahout, nRows);

		
		Matrix R = new org.apache.mahout.math.CholeskyDecomposition(centralZtZ, false).getL().transpose();
		
		
		R = PCAUtils.inv(R);
		centralYtZ=centralYtZ.times(R);
		centralYtZ=centralYtZ.transpose();
		
		org.apache.mahout.math.SingularValueDecomposition SVD = 
				new org.apache.mahout.math.SingularValueDecomposition(centralYtZ);

		return SVD;
		
	}

	/**
	 * Compute principal component analysis where the input is an
	 * RDD<org.apache.spark.mllib.linalg.Vector> of vectors such that each
	 * vector represents a row in the matrix
	 * 
	 * @param sc
	 *            Spark context that contains the configuration parameters and
	 *            represents connection to the cluster (used to create RDDs,
	 *            accumulators and broadcast variables on that cluster)
	 * @param vectors
	 *            An RDD of vectors representing the rows of the input matrix
	 * @param nRows
	 *            Number of rows in input Matrix
	 * @param nCols
	 *            Number of columns in input Matrix
	 * @param nPCs
	 *            Number of desired principal components
	 * @param errRate
	 *            The sampling rate that is used for computing the
	 *            reconstruction error
	 * @param maxIterations
	 *            Maximum number of iterations before terminating
	 * @param sketchEnable 
	 * @param writeSeed 
	 * @param inputPathSeed 
	 * @param loadSeed 
	 * @param writePC 
	 * @return Matrix of size nCols X nPCs having the desired principal
	 *         components
	 * @throws FileNotFoundException
	 */
	public static org.apache.spark.mllib.linalg.Matrix computePrincipalComponents(JavaSparkContext sc,
			JavaPairRDD<org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector> vectors,final Broadcast<Vector> br_ym_mahoutObs,
			Vector meanVector, double norm2, String outputPath, final int nRows, final int nCols, final int nPCs,
			final int subsample, double tolerance, int maxIterations,
			boolean handleMissing, boolean sketchEnable,
			boolean loadSeed, String inputPathSeed, boolean writeSeed, boolean writePC) throws FileNotFoundException {

		startTime = System.currentTimeMillis();

		Matrix Seed = null; 
		if(loadSeed){
			org.apache.spark.mllib.linalg.Matrix GaussianRandomMatrix=null;
			try {
		         FileInputStream fileIn = new FileInputStream(inputPathSeed+File.separator+"Seed.ser");
		         ObjectInputStream in = new ObjectInputStream(fileIn);
		         GaussianRandomMatrix = (org.apache.spark.mllib.linalg.Matrix ) in.readObject();
		         in.close();
		         fileIn.close();
		      } catch (IOException i) {
		         i.printStackTrace();
		         return null;
		      } catch (ClassNotFoundException c) {
		         System.out.println("Seed class not found");
		         c.printStackTrace();
		         return null;
		      }
			Seed =  PCAUtils.convertSparkToMahoutMatrix(GaussianRandomMatrix);
		}
		else{
			org.apache.spark.mllib.linalg.Matrix GaussianRandomMatrix = org.apache.spark.mllib.linalg.Matrices.randn(nCols,
					nPCs + subsample, new SecureRandom());
			Seed =  PCAUtils.convertSparkToMahoutMatrix(GaussianRandomMatrix);
			if(writeSeed){
				try {
			         FileOutputStream fileOut =
			         new FileOutputStream(outputPath+File.separator+"Seed.ser");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(GaussianRandomMatrix);
			         out.close();
			         fileOut.close();
			         System.out.printf("Seed data is saved in the output path");
			      } catch (IOException i) {
			         i.printStackTrace();
			      }
				
			}
		}
		
		// random seed for initialization
		Matrix centralC = Seed;
		double ss = RandomUtils.nextDouble();
		/************************** SSVD PART *****************************/

		/**
		 * Sketch dimension ,S=nPCs+subsample Sketched matrix, B=A*S; QR
		 * decomposition, Q=qr(B); SV decomposition, [~,s,V]=svd(Q);
		 */

		// initialize & broadcast a random seed
		// org.apache.spark.mllib.linalg.Matrix GaussianRandomMatrix =
		// org.apache.spark.mllib.linalg.Matrices.randn(nCols,
		// nPCs + subsample, new SecureRandom());
		// //PCAUtils.printMatrixToFile(GaussianRandomMatrix,
		// OutputFormat.DENSE, outputPath+File.separator+"Seed");
		// final Matrix seedMahoutMatrix =
		// PCAUtils.convertSparkToMahoutMatrix(GaussianRandomMatrix);
		/**
		 * Sketch dimension ,S=nPCs+subsample Sketched matrix, B=A*S; QR
		 * decomposition, Q=qr(B); SV decomposition, [~,s,V]=svd(Q);
		 */

		// initialize & broadcast a random seed
		
		// Matrix GaussianRandomMatrix = PCAUtils.randomValidationMatrix(nCols,
		// nPCs + subsample);
		// Matrix B = GaussianRandomMatrix;
		// PCAUtils.printMatrixToFile(PCAUtils.convertMahoutToSparkMatrix(GaussianRandomMatrix),
		// OutputFormat.DENSE, outputPath+File.separator+"Seed");

		if(sketchEnable){
			org.apache.mahout.math.SingularValueDecomposition SVD
			=sketch( sc, vectors, br_ym_mahoutObs,  nRows, nCols, nPCs,
					subsample, Seed);

			
			Matrix V = SVD.getV().viewPart(0, nCols, 0, nPCs);


			/****************************
			 * END OF SSVD
			 ***********************************/

			/**
			 * construct smart guess
			 */
			//resetting ss
			ss=0;
			// from k+1 to s
			double[] latent = new double[nPCs + subsample];

			for (int i = 0; i < (nPCs + subsample); i++) {
				latent[i] = Math.pow(SVD.getS().get(i, i), 2) / (nRows - 1);
			}

			for (int i = nPCs; i < (nPCs + subsample); i++) {
				ss += latent[i];
			}
			ss /= subsample;

			// calculate W
			// (L_tilde-ss_tilde*eye(k)).^0.5
			double[] L_ss = new double[nPCs];
			for (int i = 0; i < nPCs; i++) {
				// column major matrix, doesnt matter though symmertrical
				L_ss[i] = Math.pow((latent[i] - ss), 0.5);
			}

			double[][] centralCarray = new double[nCols][nPCs];
			for (int i = 0; i < nCols; i++) {
				for (int j = 0; j < nPCs; j++) {
					centralCarray[i][j] = V.getQuick(i, j) * L_ss[j];
				}
			}
			centralC = new DenseMatrix(centralCarray);
		}
		
		

		endTime = System.currentTimeMillis();
		totalTime = endTime - startTime;
		stat.sketchTime = (double) totalTime / 1000.0;
		stat.totalRunTime += stat.sketchTime;

		System.out.println("Rows: " + nRows + ", Columns " + nCols);

		startTime = System.currentTimeMillis();

		
		// initial CtC
		Matrix centralCtC = centralC.transpose().times(centralC);

		Matrix centralY2X = null;
		// -------------------------- EM Iterations
		// while count
		int round = 0;
		double prevObjective = Double.MAX_VALUE;
		// double error = 0;
		double relChangeInObjective = Double.MAX_VALUE;
		double prevError = Double.MAX_VALUE;
		final float threshold = 0.00001f;// not changing much
		double target_error = tolerance; // 95% accuracy
		Vector meanObsVector=meanVector;//mean of only observed values
		
		for (; (round < maxIterations && relChangeInObjective > threshold && prevError > target_error); round++) {

			// Sx = inv( ss * eye(d) + CtC );
			Matrix centralSx = centralCtC.clone();

			// Sx = inv( eye(d) + CtC/ss );
			centralSx.viewDiagonal().assign(Functions.plus(ss));
			centralSx = PCAUtils.inv(centralSx);
			
			// X = Y * C * Sx' => Y2X = C * Sx'
			centralY2X = centralC.times(centralSx.transpose());

			// Broadcast Y2X because it will be used in several jobs and several
			// iterations.
			final Broadcast<Matrix> br_centralY2X = sc.broadcast(centralY2X);
			
			final Broadcast<Vector> br_ym_mahout = sc.broadcast(meanVector);
			
			// Xm = Ym * Y2X
			Vector xm_mahout = new DenseVector(nPCs);
			xm_mahout = PCAUtils.denseVectorTimesMatrix(br_ym_mahout.value(), centralY2X, xm_mahout);

			// Broadcast Xm because it will be used in several iterations.
			final Broadcast<Vector> br_xm_mahout = sc.broadcast(xm_mahout);
			// We skip computing X as we generate it on demand using Y and Y2X

			// 3. X'X and Y'X Job: The job computes the two matrices X'X and Y'X
			/**
			 * Xc = Yc * MEM (MEM is the in-memory broadcasted matrix Y2X)
			 * 
			 * XtX = Xc' * Xc
			 * 
			 * YtX = Yc' * Xc
			 * 
			 * It also considers that Y is sparse and receives the mean vectors
			 * Ym and Xm separately.
			 * 
			 * Yc = Y - Ym
			 * 
			 * Xc = X - Xm
			 * 
			 * Xc = (Y - Ym) * MEM = Y * MEM - Ym * MEM = X - Xm
			 * 
			 * XtX = (X - Xm)' * (X - Xm)
			 * 
			 * YtX = (Y - Ym)' * (X - Xm)
			 * 
			 */
			final Accumulator<double[][]> matrixAccumXtx = sc.accumulator(new double[nPCs][nPCs],
					new MatrixAccumulatorParam());
			final Accumulator<double[][]> matrixAccumYtx = sc.accumulator(new double[nCols][nPCs],
					new MatrixAccumulatorParam());
			final Accumulator<double[]> matrixAccumX = sc.accumulator(new double[nPCs], new VectorAccumulatorParam());

			/*
			 * Initialize the output matrices and vectors once in order to avoid
			 * generating massive intermediate data in the workers
			 */
			final double[][] resArrayYtX = new double[nCols][nPCs];
			final double[][] resArrayXtX = new double[nPCs][nPCs];
			final double[] resArrayX = new double[nPCs];

			/*
			 * Used to sum the vectors in one partition.
			 */
			final double[][] internalSumYtX = new double[nCols][nPCs];
			final double[][] internalSumXtX = new double[nPCs][nPCs];
			final double[] internalSumX = new double[nPCs];

			vectors.foreachPartition(new VoidFunction<Iterator<
					Tuple2<org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector>>>() {

				public void call(Iterator<Tuple2<org.apache.spark.mllib.linalg.Vector,
						org.apache.spark.mllib.linalg.Vector>> arg0) throws Exception {
					Tuple2<org.apache.spark.mllib.linalg.Vector,
					org.apache.spark.mllib.linalg.Vector>  yi;
					org.apache.spark.mllib.linalg.Vector yObsi,yMissi;
					while (arg0.hasNext()) {
						yi=arg0.next();
						yObsi =yi._1;
						yMissi = yi._2;
						
						/*
						 * Perform in-memory matrix multiplication xi = yi' *
						 * Y2X
						 */
						PCAUtils.sparseMissVectorTimesMatrix(yObsi, yMissi, br_centralY2X.value(), resArrayX);
						
						// get only the sparse indices
						int[] indicesObs = ((SparseVector) yObsi).indices();
						int[] indicesMiss = ((SparseVector) yMissi).indices();

						PCAUtils.outerProductWithMissIndices(yObsi, yMissi, br_ym_mahout.value(), resArrayX, br_xm_mahout.value(),
								resArrayYtX, indicesObs, indicesMiss);
						PCAUtils.outerProductArrayInput(resArrayX, br_xm_mahout.value(), resArrayX,
								br_xm_mahout.value(), resArrayXtX);
						int i, j, rowIndexYtX;

						// add the sparse indices only
						for (i = 0; i < indicesObs.length; i++) {
							rowIndexYtX = indicesObs[i];
							for (j = 0; j < nPCs; j++) {
								internalSumYtX[rowIndexYtX][j] += resArrayYtX[rowIndexYtX][j];
								resArrayYtX[rowIndexYtX][j] = 0; // reset it
							}

						}
						for (i = 0; i < indicesMiss.length; i++) {
							rowIndexYtX = indicesMiss[i];
							for (j = 0; j < nPCs; j++) {
								internalSumYtX[rowIndexYtX][j] += resArrayYtX[rowIndexYtX][j];
								resArrayYtX[rowIndexYtX][j] = 0; // reset it
							}

						}						
						for (i = 0; i < nPCs; i++) {
							internalSumX[i] += resArrayX[i];
							for (j = 0; j < nPCs; j++) {
								internalSumXtX[i][j] += resArrayXtX[i][j];
								resArrayXtX[i][j] = 0; // reset it
							}

						}
					}
					matrixAccumX.add(internalSumX);
					matrixAccumXtx.add(internalSumXtX);
					matrixAccumYtx.add(internalSumYtX);
				}

			});// end X'X and Y'X Job

			/*
			 * Get the values of the accumulators.
			 */
			Matrix centralYtX = new DenseMatrix(matrixAccumYtx.value());
			Matrix centralXtX = new DenseMatrix(matrixAccumXtx.value());
			Vector centralSumX = new DenseVector(matrixAccumX.value());

			/*
			 * Mi = (Yi-Ym)' x (Xi-Xm) = Yi' x (Xi-Xm) - Ym' x (Xi-Xm)
			 * 
			 * M = Sum(Mi) = Sum(Yi' x (Xi-Xm)) - Ym' x (Sum(Xi)-N*Xm)
			 * 
			 * The first part is done in the previous job and the second in the
			 * following method
			 */
			centralYtX = PCAUtils.updateXtXAndYtx(centralYtX, centralSumX, br_ym_mahout.value(), xm_mahout, nRows);
			centralXtX = PCAUtils.updateXtXAndYtx(centralXtX, centralSumX, xm_mahout, xm_mahout, nRows);

			// XtX = X'*X + ss * Sx
			final double finalss = ss;
			centralXtX.assign(centralSx, new DoubleDoubleFunction() {
				@Override
				public double apply(double arg1, double arg2) {
					return arg1 + finalss * arg2*nRows;
				}
			});

			// C = (Ye'*X) / SumXtX;
			
			Matrix invXtX_central = PCAUtils.inv(centralXtX);
//			PCAUtils.printMatrixToFile(PCAUtils.convertMahoutToSparkMatrix(invXtX_central),
//					OutputFormat.DENSE, outputPath, "invXtX_central"+round+".txt");
//			PCAUtils.printMatrixToFile(PCAUtils.convertMahoutToSparkMatrix(centralYtX),
//					OutputFormat.DENSE, outputPath, "centralYtX"+round+".txt");
			Matrix oldcentralC=centralC;			
			centralC = centralYtX.times(invXtX_central);
			
			centralCtC = centralC.transpose().times(centralC);
			
			if(writePC){
				Matrix PCs=new 
						org.apache.mahout.math.SingularValueDecomposition(centralC).getU();
				
				PCAUtils.printMatrixToFile(PCAUtils.convertMahoutToSparkMatrix(PCs),
						OutputFormat.DENSE, outputPath, "V"+round+".txt");
			}
			
			// Compute new value for ss
			// ss = ( sum(sum(Ye.^2)) + trace(XtX*CtC) - 2sum(XiCtYit))/(N*D);

			// 4. Variance Job: Computes part of variance that requires a
			// distributed job
			/**
			 * xcty = Sum (xi * C' * yi')
			 * 
			 * We also regenerate xi on demand by the following formula:
			 * 
			 * xi = yi * y2x
			 * 
			 * To make it efficient for sparse matrices that are not
			 * mean-centered, we receive the mean separately:
			 * 
			 * xi = (yi - ym) * y2x = yi * y2x - xm, where xm = ym*y2x
			 * 
			 * xi * C' * (yi-ym)' = xi * ((yi-ym)*C)' = xi * (yi*C - ym*C)'
			 * 
			 */

			double ss2 = PCAUtils.trace(centralXtX.times(centralCtC));
			final double[] resArrayYmC = new double[centralC.numCols()];
			PCAUtils.denseVectorTimesMatrix(meanVector, centralC, resArrayYmC);
			final Broadcast<Matrix> br_centralC = sc.broadcast(centralC);
			final double[] resArrayYiC = new double[centralC.numCols()];
			final Accumulator<Double> doubleAccumXctyt = sc.accumulator(0.0);
			
			
			if(handleMissing){
				vectors=vectors.mapToPair(new PairFunction<Tuple2<org.apache.spark.mllib.linalg.Vector,
						org.apache.spark.mllib.linalg.Vector>,
								org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector>(){

						@Override
						public Tuple2<org.apache.spark.mllib.linalg.Vector, org.apache.spark.mllib.linalg.Vector> call(
								Tuple2<org.apache.spark.mllib.linalg.Vector, org.apache.spark.mllib.linalg.Vector> t) throws Exception {
							org.apache.spark.mllib.linalg.Vector yObsi=t._1;
							org.apache.spark.mllib.linalg.Vector yMissi=t._2;
							
							PCAUtils.sparseMissVectorTimesMatrix(yObsi,yMissi,
									br_centralY2X.value(), resArrayX);
							PCAUtils.sparseMissVectorTimesMatrix(yObsi, yMissi,
									br_centralC.value(), resArrayYiC);

							PCAUtils.subtract(resArrayYiC, resArrayYmC);
							double dotRes = PCAUtils.dot(resArrayX, resArrayYiC);
							doubleAccumXctyt.add(dotRes); 					
													
							org.apache.spark.mllib.linalg.Vector updatedYMissi=
									PCAUtils.reconMissValue(yMissi, 
											br_ym_mahout.value(), resArrayX,  
											br_xm_mahout.value(), br_centralC.value());
											
							return new Tuple2<org.apache.spark.mllib.linalg.Vector,
									org.apache.spark.mllib.linalg.Vector>(yObsi, updatedYMissi);
							// TODO Auto-generated method stub		
						}
					}
				).persist(StorageLevel.MEMORY_ONLY_SER());
				

				final Accumulator<double[]> matrixAccumY = sc.accumulator(new double[nCols], new VectorAccumulatorParam());
				final double[] internalSumY = new double[nCols];
				
				vectors.foreachPartition(new VoidFunction<Iterator<
						Tuple2<org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector>>>() {

					public void call(Iterator<Tuple2<org.apache.spark.mllib.linalg.Vector,
							org.apache.spark.mllib.linalg.Vector>> arg0) throws Exception {
						org.apache.spark.mllib.linalg.Vector yMissi;
						int[] indicesMiss = null;
						int i;
						while (arg0.hasNext()) {
							Tuple2 <org.apache.spark.mllib.linalg.Vector,
							org.apache.spark.mllib.linalg.Vector> entry= arg0.next();
							yMissi= entry._2;
							indicesMiss = ((SparseVector) yMissi).indices();
							
							for (i = 0; i < indicesMiss.length; i++) {
								internalSumY[indicesMiss[i]] += yMissi.apply(indicesMiss[i]);
							}
						}
						matrixAccumY.add(internalSumY);
					}

				});// End Mean Job
				
				meanVector=new DenseVector(matrixAccumY.value()).divide(nRows);
				meanVector=meanVector.plus(meanObsVector);
				System.out.println(meanVector);
				//br_ym_mahout.destroy();
			}
			
			
			
			
			double xctyt = doubleAccumXctyt.value();
			ss = (norm2 + ss2 - 2 * xctyt) / (nRows * nCols);
			// log.info("SSSSSSSSSSSSSSSSSSSSSSSSSSSS " + ss + " (" + norm2 + "
			// + "+ ss2 + " -2* " + xctyt);

			// log.info("Objective: %.6f relative change: %.6f \n",
			// objective,relChangeInObjective);

			endTime = System.currentTimeMillis();
			totalTime = endTime - startTime;
			stat.ppcaIterTime.add((double) totalTime / 1000.0);
			stat.totalRunTime += (double) totalTime / 1000.0;
			
			//dw = max(max(abs(W-Wnew) / (sqrt(eps)+max(max(abs(Wnew))))));
			
			double maxWnew=0,dw=0;
			
			for(int a=0;a<nCols;a++){
				for(int b=0;b<nPCs;b++){
					maxWnew=Math.max(Math.abs(centralC.getQuick(a,b)),maxWnew);
					dw=Math.max(
							Math.abs(oldcentralC.getQuick(a,b))
									-Math.abs(centralC.getQuick(a,b)),dw);
				}
			}
			
			double sqrtEps=2.2204e-16;
			dw/=(sqrtEps+maxWnew);
			stat.errorList.add(dw);
			System.out.println("dw "+dw);
			
			if(dw<tolerance) break;
			

			
			
			//if(dw<=tolerance) break;
			/**
			 * reinitialize
			 */
			startTime = System.currentTimeMillis();
			
		}
		// return the actual PC not the principal subspace
		stat.nIter = round;
		
		// count the average ppca runtime

		for (int j = 0; j < stat.ppcaIterTime.size(); j++) {
			stat.avgppcaIterTime += stat.ppcaIterTime.get(j);
		}
		stat.avgppcaIterTime /= stat.ppcaIterTime.size();

		// save statistics
		PCAUtils.printStatToFile(stat, outputPath);
				
		return PCAUtils
				.convertMahoutToSparkMatrix(new org.apache.mahout.math.SingularValueDecomposition(centralC).getU());

	}

	private static void printLogMessage(String argName) {
		log.error("Missing arguments -D" + argName);
		log.info(
				"Usage: -Di=<path/to/input/matrix> -Do=<path/to/outputfolder> -Drows=<number of rows> -Dcols=<number of columns> -Dpcs=<number of principal components> [-DerrSampleRate=<Error sampling rate>] [-DmaxIter=<max iterations>] [-DoutFmt=<output format>] ");
	}
}