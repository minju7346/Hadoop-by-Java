package com.minju.min;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class AbandonedPetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	static Logger logger = Logger.getLogger(AbandonedPetMapper.class);
	
	int iCaseTotal = 0;
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		PropertyConfigurator.configure("log4j.properties");
		Text species;
		String str_caseTotal;
		
		String line = ivalue.toString();
		String PetDate[] = line.split(",");
		
		species = new Text(PetDate[7]);
		String check = PetDate[7];
		str_caseTotal = "1";
		
		if(isNumberic(check) == false) {
			iCaseTotal = (int)Double.parseDouble(str_caseTotal);
			context.write(species,  new IntWritable(iCaseTotal));
		}		
		
	}
	
	public static boolean isNumberic(String strNum) {
		if(strNum == null) {
			return false;
		}
		try {
			double d = Double.parseDouble(strNum);
		}catch(NumberFormatException nfe) {
			return false;
		}
		return true;
	}

}
