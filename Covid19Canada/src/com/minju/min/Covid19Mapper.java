package com.minju.min;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Covid19Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	static Logger logger = Logger.getLogger(Covid19Mapper.class);
	
	int iCaseTotal = 0;
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
			
		PropertyConfigurator.configure("log4j.properties");
		Text date;
		String str_caseTotal;
			
		String line = ivalue.toString();
		String CovidDate[] = line.split(",");
			
		date = new Text(CovidDate[3]);
		str_caseTotal = CovidDate[8];

		if(isNumberic(str_caseTotal) == true) {
			iCaseTotal = (int)Double.parseDouble(str_caseTotal);
			context.write(date, new IntWritable(iCaseTotal));
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
