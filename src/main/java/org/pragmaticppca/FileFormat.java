package org.pragmaticppca;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


public class FileFormat {
	 private final static Logger log = LoggerFactory.getLogger(FileFormat.class);
	 public enum OutputFormat {
		DENSE,  //Dense matrix 
		LIL, //List of lists
		COO, //Coordinate List
	 } 
	 public enum InputFormat {
		DENSE,
		COO,
		MISSCOO,
		COONETFLIX,
		PRINTNETFIX
	 } 
	 public static void main(String[] args) {
		final String inputPath;
		final int cardinality;
		final String outputPath;
		final InputFormat inputFormat;
		try {
			inputPath=System.getProperty("Input");
			if(inputPath==null)
				throw new IllegalArgumentException();
		}
		catch(Exception e) {
			printLogMessage("Input");
			return;
		}
		try {
			inputFormat=InputFormat.valueOf(System.getProperty("InputFmt"));
		}
		catch(IllegalArgumentException e) {
		    	 log.warn("Invalid Format " + System.getProperty("InputFmt") );
		    	 return;
		}
		catch(Exception e) {
		    	printLogMessage("InputFmt");
		    	return;
		}
		try {
			outputPath=System.getProperty("Output");
			if(outputPath==null)
				throw new IllegalArgumentException();
			File outputFile=new File(outputPath);
			if( outputFile.isFile() || outputFile==null )
			{
				log.error("Output Path must be a directory, " + outputPath + " is either not a directory or not a valid path");
				return;
			}
		}
		catch(Exception e) {
			printLogMessage("Output");
			return;
		}
		try {
			cardinality=Integer.parseInt(System.getProperty("Cardinality"));
		}
		catch(Exception e) {
			printLogMessage("Cardinality");
			return;
		}
		int base=-1;
		try {
			base=Integer.parseInt(System.getProperty("Base"));
		}
		catch(Exception e) {
			log.warn("It is not specified whether the input is zero-based or one-based, this parameter is useful only if the input is in COO format");
		}
		double perc=10;
		try {
			perc=Integer.parseInt(System.getProperty("Perc"));
		}
		catch(Exception e) {
			log.warn("It is not specified whether the input is percentage this parameter is useful only if the input is in COO format");
		}
		
		switch(inputFormat)
		{
			case COO:
				if(base==-1) {
					log.error("You have to specify whether the rows and columns IDs start with 0 or 1 using the argument -DBase");
					return;
				}
				convertFromCooToSeq(inputPath,cardinality,base,outputPath);
				break;
			case DENSE:
				convertFromDenseToSeq(inputPath,cardinality,outputPath);
				break;
			case MISSCOO:
				createMissingIDCOO(perc, inputPath, cardinality, base, outputPath);
				break;
			case COONETFLIX:
				convertNetFlix(inputPath, cardinality, base, outputPath);
				break;
			case PRINTNETFIX:
				printMissingIDNetFlix(perc, inputPath, cardinality, base, outputPath);
				break;
				
		}
		
		
	}
	public static void convertFromDenseToSeq(String inputPath, int cardinality, String outputFolderPath)
	{
		try
    	{
	    	 final Configuration conf = new Configuration();
	         final FileSystem fs = FileSystem.get(conf);
	         SequenceFile.Writer writer;
	
	         final IntWritable key = new IntWritable();
	         final VectorWritable value = new VectorWritable();
	         
	         int lineNumber=0;
	         String thisLine;
	         File[] filePathList=null;
	         File inputFile=new File(inputPath);
	          if(inputFile.isFile()) // if it is a file
	          { 
	        	  filePathList= new File[1];
	        	  filePathList[0]=inputFile;
	          }
	          else
	          {
	        	  filePathList=inputFile.listFiles();
	          }
	          if(filePathList==null)
	          {
	        	  log.error("The path " + inputPath + " does not exist");
	          	  return;
	          }
	          for(File file:filePathList)
	          {
		          BufferedReader br = new BufferedReader(new FileReader(file));
		          Vector vector = null;
		          String outputFileName=outputFolderPath+ File.separator + file.getName() + ".seq";
		          writer=SequenceFile.createWriter(fs, conf, new Path(outputFileName), IntWritable.class, VectorWritable.class, CompressionType.BLOCK);
		          while ((thisLine = br.readLine()) != null) { // while loop begins here
		              if(thisLine.isEmpty())
		            	  continue;
		        	  String [] splitted = thisLine.split("\\s+");
		        	  vector = new SequentialAccessSparseVector(splitted.length);
		        	  for (int i=0; i < splitted.length; i++)
		        	  {
		        		  vector.set(i, Double.parseDouble(splitted[i]));
		        	  }
		        	  key.set(lineNumber);
		        	  value.set(vector);
		        	  //System.out.println(vector);
		        	  writer.append(key,value);//write last row
		        	  lineNumber++;
		          }
		          writer.close();
	          }   
		    }
	    	catch (Exception e) {
	    		e.printStackTrace();
	    	}
		
	    	
	}
	
	public static void convertFromCooToSeq(String inputPath, int cardinality, int base, String outputFolderPath){

    	try
    	{
    	 final Configuration conf = new Configuration();
         final FileSystem fs = FileSystem.get(conf);
         SequenceFile.Writer writer=null;

         final IntWritable key = new IntWritable();
         final VectorWritable value = new VectorWritable();
         
         Vector vector = null;
    
          String thisLine;
          
          int lineNumber=0;
          int prevRowID=-1;
          boolean first=true;
          File[] filePathList=null;
	      File inputFile=new File(inputPath);
          if(inputFile.isFile()) // if it is a file
          { 
        	  filePathList= new File[1];
        	  filePathList[0]=inputFile;
          }
          else
          {
        	  filePathList=inputFile.listFiles();
          }
          if(filePathList==null)
          {
        	  log.error("The path " + inputPath + " does not exist");
          	  return;
          }
          int count=1;
          
          Map<Integer, ArrayList<Integer>> id=new HashedMap();

          for(File file:filePathList)
          {
        	  BufferedReader br = new BufferedReader(new FileReader(file));
        	  String outputFileName=outputFolderPath+ File.separator + file.getName() + ".seq";
	          writer=SequenceFile.createWriter(fs, conf, new Path(outputFileName), IntWritable.class, VectorWritable.class, CompressionType.BLOCK);
	          
	          while ((thisLine = br.readLine()) != null) { // while loop begins here  
	        	  String [] splitted = thisLine.split("\\s+");
	        	  int colID=Integer.parseInt(splitted[1]);
	        	  //if(count==1000) break;//take first 5000 count line number
	        	  if(colID>cardinality) continue;//take 5000 columns
	        	  count++;
	        	  lineNumber++;
	          }

	         
	          int nnzElems=lineNumber;
	          System.out.println(nnzElems);
				int size=(int) (nnzElems*30/100);
				Random r = new Random();
				count=0;
				//randomly choose a row, then randomly choose the serial of a nonzero element
				//using map for less memory, and ease of search
				
				Set<Integer> generated = new LinkedHashSet<Integer>();
				while (generated.size() < size)
				{
				    Integer next = r.nextInt(nnzElems);
				    // As we're adding to a set, this will automatically do a containment check
				    generated.add(next);
				    count++;
				}
			  System.out.println(count);
	          lineNumber=0;
	          count=0;
	          br.close();
	          int check=0;
	          
	          br = new BufferedReader(new FileReader(file));
	          
	          while ((thisLine = br.readLine()) != null) { // while loop begins here 
	        	  String [] splitted = thisLine.split("\\s+");
	        	  int rowID=Integer.parseInt(splitted[0]);
	        	  int colID=Integer.parseInt(splitted[1]);
	        	  double element=1;//Double.parseDouble(splitted[2]);
	        	  //if(count==1000) break;//take first 5000 count line number
	        	  if(colID>cardinality) continue;//take 5000 column     	  
	        	        
	        	  if(first)
	        	  {
	        		  first=false;
	        		  vector = new SequentialAccessSparseVector(cardinality);
	        	  }
	        	  else if(rowID != prevRowID)
	        	  {
	        		 
	        		  key.set(count++);
	        		  value.set(vector);
	            	  //System.out.println(vector);
	        		  
	            	  writer.append(key,value);//write last row
	            	  vector = new SequentialAccessSparseVector(cardinality);
	        	  }
	        	  prevRowID=rowID;
	        	  vector.set(colID-base,element);
	        	  
	        	  if(generated.contains((Integer)lineNumber)){
	        		  if(id.get(count)!=null){
	      					id.get(count).add(colID-base);
	      					check++;
	      				}
		      			else{
		      				ArrayList<Integer> colEntry=new ArrayList<Integer>();
		      				colEntry.add(colID-base);
		      				id.put(new Integer(count), colEntry);
		      				check++;
		      			}
	        	  }
	        	  lineNumber++;
	          }
          }
          if(writer!=null) //append last vector in last file
          {
        	  
	          key.set(count);
	          value.set(vector);
	    	  writer.append(key,value);//write last row
	    	  System.out.println("Total number of rows"+count+"");
	          writer.close();
          }
          try {
		         FileOutputStream fileOut =
		         new FileOutputStream(outputFolderPath+File.separator+"mapMissingIDs.ser");
		         ObjectOutputStream out = new ObjectOutputStream(fileOut);
		         out.writeObject(id);
		         out.close();
		         fileOut.close();
		         System.out.printf("Serialized data is saved in the output path");
		      } catch (IOException i) {
		         i.printStackTrace();
		      }
          
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }
	
	
	
	
	
	
	
	public static void convertNetFlix(String inputPath, int cardinality, int base, String outputFolderPath){

    	try
    	{
    	 final Configuration conf = new Configuration();
         final FileSystem fs = FileSystem.get(conf);
         SequenceFile.Writer writer=null;

         final IntWritable key = new IntWritable();
         final VectorWritable value = new VectorWritable();
         
         Vector vector = null;
    
          String thisLine;
          
          File[] filePathList=null;
	      File inputFile=new File(inputPath);
          if(inputFile.isFile()) // if it is a file
          { 
        	  filePathList= new File[1];
        	  filePathList[0]=inputFile;
          }
          else
          {
        	  filePathList=inputFile.listFiles();
          }
          if(filePathList==null)
          {
        	  log.error("The path " + inputPath + " does not exist");
          	  return;
          }
          
          Map<Integer, ArrayList<Tuple2<Integer,Double>>> id=new HashMap();
          int colID=-1;
          int nnz=0;
          int count=453186;
          String outputFileName=outputFolderPath+ File.separator +"Netflix"+count+".seq";
	         
          writer=SequenceFile.createWriter(fs, conf, 
        		  new Path(outputFileName), IntWritable.class, VectorWritable.class, CompressionType.BLOCK);
          for(File file:filePathList)
          {
        	  BufferedReader br = new BufferedReader(new FileReader(file));
        	  colID++;
	        
	          
	          br = new BufferedReader(new FileReader(file));
	          thisLine = br.readLine();
	          while ((thisLine = br.readLine()) != null) { // while loop begins here 
	        	  String [] splitted = thisLine.split(",");
	        	  int rowID=Integer.parseInt(splitted[0]);
	        	 // int colID=Integer.parseInt(splitted[1]);
	        	  double element=Double.parseDouble(splitted[1]);
	        	  //if(count==1000) break;//take first 5000 count line number
	        	  if(rowID<2500000||rowID>2649429) continue;//take 5000 column     	  
	        	
	        	  Tuple2<Integer,Double> entry=new Tuple2<Integer, Double>(colID, element);
	        	  
        		  if(id.get(rowID)!=null){
      					id.get(rowID).add(entry);
      				}
	      			else{
	      				ArrayList<Tuple2<Integer,Double>> colEntry=
	      						new ArrayList<Tuple2<Integer,Double>>();
	      				colEntry.add(entry);
	      				id.put(new Integer(rowID), colEntry);
	      				count++;
	      			}
	        	  
	          }
          }
          System.out.println(colID);
          
          
          for (int rowI=2500000;rowI<2649429;rowI++)
          {       
              vector =new SequentialAccessSparseVector(cardinality);
              if(id.containsKey(rowI)){
            	  for(int i=0;i<id.get(rowI).size();i++){
                	  Tuple2<Integer,Double> element=id.get(rowI).get(i);
                	  vector.set(element._1, element._2);
                	  nnz++;
                  }
              }
              
              key.set(rowI);
    		  value.set(vector);
        	  //System.out.println(vector);
    		  
        	  writer.append(key,value);//write last row
          }
          System.out.println("Total number of rows"+count+"");
          System.out.println("Total number of nnz elements"+nnz);
          writer.close();
          
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }
	
	public static void printMissingIDNetFlix(double perc, String inputPath, int cardinality, int base, String outputFolderPath){

    	try
    	{
    	 final Configuration conf = new Configuration();
         final FileSystem fs = FileSystem.get(conf);
 
          String thisLine;
          
          int lineNumber=0;
          int prevRowID=-1;
          boolean first=true;
          File[] filePathList=null;
	      File inputFile=new File(inputPath);
          if(inputFile.isFile()) // if it is a file
          { 
        	  filePathList= new File[1];
        	  filePathList[0]=inputFile;
          }
          else
          {
        	  filePathList=inputFile.listFiles();
          }
          if(filePathList==null)
          {
        	  log.error("The path " + inputPath + " does not exist");
          	  return;
          }
          
          Map<Integer, ArrayList<Integer>> id=new HashedMap();
    	  int count=0;
    	  
          for(File file:filePathList)
          {
        	  BufferedReader br = new BufferedReader(new FileReader(file));
        	  String outputFileName=outputFolderPath+ File.separator + file.getName() + ".seq";
	         
        	  int colID=1,rowID;
        
        	  String [] splitted;
	          while ((thisLine = br.readLine()) != null) { // while loop begins here  
	        	  if(thisLine.charAt(thisLine.length()-1)==':'){
	        		  colID=Integer.parseInt(thisLine.substring(0, thisLine.length()-1));
	        		  continue;
	        	  }
	        	  else{
	        		  splitted = thisLine.split(",");
	        		  rowID=Integer.parseInt(splitted[0]);
	        		  count++;
					  if(id.get(rowID)!=null){
							id.get(rowID).add(colID-base);
						}
						else{
							ArrayList<Integer> colEntry=new ArrayList<Integer>();
							colEntry.add(colID-base);
							id.put(new Integer(rowID), colEntry);
						}
	        	  }
	        	  
	        	  
	        	 
	        	  //if(count==1000) break;//take first 5000 count line number
	        	  //if(colID>cardinality) continue;//take 5000 columns
	        	 
	          }          
	        	  
			
			 
	        	 
          }
          System.out.println(count);
          try {
		         FileOutputStream fileOut =
		         new FileOutputStream(outputFolderPath+File.separator+"mapMissingIDsNetFlix.ser");
		         ObjectOutputStream out = new ObjectOutputStream(fileOut);
		         out.writeObject(id);
		         out.close();
		         fileOut.close();
		         System.out.printf("Serialized data is saved in the output path");
		      } catch (IOException i) {
		         i.printStackTrace();
		      }
          
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }

	
	public static void createMissingIDCOO(double perc, String inputPath, int cardinality, int base, String outputFolderPath){

    	try
    	{
    	 final Configuration conf = new Configuration();
         final FileSystem fs = FileSystem.get(conf);
 
          String thisLine;
          
          int lineNumber=0;
          int prevRowID=-1;
          boolean first=true;
          File[] filePathList=null;
	      File inputFile=new File(inputPath);
          if(inputFile.isFile()) // if it is a file
          { 
        	  filePathList= new File[1];
        	  filePathList[0]=inputFile;
          }
          else
          {
        	  filePathList=inputFile.listFiles();
          }
          if(filePathList==null)
          {
        	  log.error("The path " + inputPath + " does not exist");
          	  return;
          }
          int count=1;
          
          Map<Integer, ArrayList<Integer>> id=new HashedMap();

          for(File file:filePathList)
          {
        	  BufferedReader br = new BufferedReader(new FileReader(file));
        	  String outputFileName=outputFolderPath+ File.separator + file.getName() + ".seq";
	         
	          
	          while ((thisLine = br.readLine()) != null) { // while loop begins here  
	        	  String [] splitted = thisLine.split(",");
	        	  int colID=Integer.parseInt(splitted[1]);
	        	  //if(count==1000) break;//take first 5000 count line number
	        	  if(colID>cardinality) continue;//take 5000 columns
	        	  count++;
	        	  lineNumber++;
	          }

	         
	          int nnzElems=lineNumber;
	          System.out.println(nnzElems);
				int size=(int) (nnzElems*perc/100);
				Random r = new Random();
				count=0;
				//randomly choose a row, then randomly choose the serial of a nonzero element
				//using map for less memory, and ease of search
				
				Set<Integer> generated = new LinkedHashSet<Integer>();
				while (generated.size() < size)
				{
				    Integer next = r.nextInt(nnzElems);
				    // As we're adding to a set, this will automatically do a containment check
				    generated.add(next);
				    count++;
				}
			  System.out.println(count);
	          lineNumber=0;
	          count=0;
	          br.close();
	          int check=0;
	          
	          br = new BufferedReader(new FileReader(file));
	          
	          while ((thisLine = br.readLine()) != null) { // while loop begins here 
	        	  String [] splitted = thisLine.split(",");
	        	  int rowID=Integer.parseInt(splitted[0]);
	        	  int colID=Integer.parseInt(splitted[1]);
	        	  //if(count==1000) break;//take first 5000 count line number
	        	  if(colID>cardinality) continue;//take 5000 column     	  
	        	        
	        	  if(first)
	        	  {
	        		  first=false;
	        	  }
	        	  else if(rowID != prevRowID)
	        	  {
	        		 
	        		count++;
	            	  //System.out.println(vector);
	        	  }
	        	  prevRowID=rowID;
	        	  
	        	  if(generated.contains((Integer)lineNumber)){
	        		  if(id.get(count)!=null){
	      					id.get(count).add(colID-base);
	      					check++;
	      				}
		      			else{
		      				ArrayList<Integer> colEntry=new ArrayList<Integer>();
		      				colEntry.add(colID-base);
		      				id.put(new Integer(count), colEntry);
		      				check++;
		      			}
	        	  }
	        	  lineNumber++;
	          }
          }
          try {
		         FileOutputStream fileOut =
		         new FileOutputStream(outputFolderPath+File.separator+"mapMissingIDs.ser");
		         ObjectOutputStream out = new ObjectOutputStream(fileOut);
		         out.writeObject(id);
		         out.close();
		         fileOut.close();
		         System.out.printf("Serialized data is saved in the output path");
		      } catch (IOException i) {
		         i.printStackTrace();
		      }
          
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }

	
	private static void printLogMessage(String argName )
	 {
		log.error("Missing arguments -D" + argName);
		log.info("Usage: -DInput=<path/to/input/matrix> -DOutput=<path/to/outputfolder> -DInputFmt=<DENSE/COO> -DCardinaality=<number of columns> [-DBase=<0/1>(0 if input is zero-based, 1 if input is 1-based]"); 
	 }
	
}
