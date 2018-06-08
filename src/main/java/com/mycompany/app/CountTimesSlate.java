
//input from NewForm

package com.mycompany.app;

import java.io.IOException;
import java.util.StringTokenizer;


import java.text.DecimalFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.*;

public class CountTimesSlate {

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			String[] temp,help;
			int i;
			int s=0;
			while (itr.hasMoreTokens()) {
				
		    	temp = (itr.nextToken()).split("\t");
		    	

		    	
		    	if (Integer.parseInt(temp[temp.length-1])>=0){
		    	
		    	for (i=1 ; i<Integer.parseInt(temp[temp.length-1])+1 ; i++){
		    			
		    			context.write(new Text(temp[i]), new Text ("1"));
		    			System.out.println("Eimai sthn "+i+" epanalipsi kai timi "+temp[i]);
		    		
		    	}
		    	context.write(new Text(temp[Integer.parseInt(temp[temp.length-1])+1]), new Text ("5"));
		    	System.out.println("Eimai sthn "+Integer.parseInt(temp[temp.length-1])+1+" epanalipsi kai timi "+temp[Integer.parseInt(temp[temp.length-1])+1]);
		    		
		    //	if (Integer.parseInt(temp[temp.length-1])>=0){
		    		

		    	for (i=Integer.parseInt(temp[temp.length-1])+2 ; i<11 ; i++){
		    		
		    			context.write(new Text(temp[i]), new Text ("2"));
		    			System.out.println("Eimai sthn "+i+" epanalipsi kai timi "+temp[i]);
		    		
		    	}
		    /*	}
		    	else {
		    		for (i=1;i<11;i++){
		    			
		    			context.write(new Text(temp[i]), new Text ("2"));
		    		}
		    	}

		    */	for (i=11 ; i<temp.length-1 ; i++){
		    		
		    		context.write(new Text(temp[i]), new Text ("3"));
		    		System.out.println("Eimai sthn "+i+" epanalipsi kai timi "+temp[i]);
		    		
		    	}
		    }
		    else{
		    	for (i=1;i<11;i++){
		    			
		    			context.write(new Text(temp[i]), new Text ("2"));
		    		}
		    }
		    	
		    	
		    	
		    
		    }
		}
	}

	public static class IntSumReducer 
	    extends Reducer<Text,Text,Text,Text> {
		private IntWritable result = new IntWritable();
	
		public void reduce(Text key, Iterable<Text> values,
		            Context context
                    ) throws IOException, InterruptedException {
			int pro=0,sun=0,click=0,contin=0; //sun counts the total number of times provlithike and pro only the times it was seen according to the cascade
			int thres=1;
			Text two = new Text("1");


			for (Text val : values) {

			if ((val.toString()).equals("3")){
				click++;
			}
			else{
			sun++;
			if ((val.toString()).equals("5")){
				pro++;
			}
			
			else if ((val.toString()).equals("1"))
			{
			 contin++;
			 pro++;	
			}


			}
			}

			DecimalFormat df = new DecimalFormat("#.####");
			
			
			
			
			
			if (sun>thres && pro>1){
		    context.write(key, new Text (String.valueOf(sun)+"\t"+String.valueOf(pro)+"\t"+String.valueOf(contin)+"\t"+String.valueOf(click)+"\t"+String.valueOf(df.format(click*1.0/sun))+"\t"+String.valueOf(df.format(click*1.0/pro))+"\t"+String.valueOf(df.format(contin*1.0/pro))));				
			}
			else{
				context.write(key, new Text (String.valueOf(sun)+"\t"+String.valueOf(pro)+"\t"+String.valueOf(contin)+"\t"+String.valueOf(click)+"\t"+String.valueOf(0.0)+"\t"+String.valueOf(0.0)+"\t"+String.valueOf(0.0)));				
			
			}

		}
	}	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: com.apache.hadoop.MapReduce.DateCounter <in> [<in>...] <out>");
		    System.exit(2);
		}
		    
		Job job = new Job(conf, "DayCounter");
		job.setJarByClass(CountTimesSlate.class);
		job.setJobName("DayCounter");
		    
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
		new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}

