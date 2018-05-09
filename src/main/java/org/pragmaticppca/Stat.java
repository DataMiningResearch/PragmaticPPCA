package org.pragmaticppca;

import java.util.ArrayList;

public class Stat {
	public String appName=null;
	public String dataSet=null;
	public int nRows=0;
	public int nCols=0;
	public int nIter=0;
	public double totalRunTime=0;
	public double preprocessTime=0;
	public double sketchTime=0;
	public double avgppcaIterTime=0;
	public double avgSketchIterTime=0;
	public ArrayList<Double> ppcaIterTime=new ArrayList<Double>();
	public ArrayList<Double> sketchIterTime=new ArrayList<Double>();
	public ArrayList<Double> errorList=new ArrayList<Double>();
	Stat(){
		
	}
	
}
