package com.minju.min;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class AbandonedPetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	static Logger logger = Logger.getLogger(AbandonedPetReducer.class);
	
	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//int maxValue = Integer.MIN_VALUE;
		logger.info("_key : "+_key+", values"+values);
		
		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
			logger.info("val : " +val+ ", values" + total);
		}
		
		context.write(_key, new IntWritable(total));
	}

}
