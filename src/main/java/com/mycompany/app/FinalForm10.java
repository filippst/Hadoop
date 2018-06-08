//2 inputs from ToBinaryOutput and AllStats
//tautoxrona tha petaksw ta clicks afou den ta thelw pia

package com.mycompany.app;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.*;

public class FinalForm10 {
public static int thesi = 10;

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			String[] temp,help;
			int pos=0;
			
			while (itr.hasMoreTokens()) {
				
		    	temp = (itr.nextToken()).split("\\s+");
		    	
		    		
		    		if (temp.length<10){
		    		context.write(new Text(temp[0]),value);
		    		}
		    		else{
		    			
		    			context.write(new Text (temp[10+thesi]),value);
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
			
			String sum ="",endiamesos="";
			String[] help = new String[30];
			
			ArrayList<String> sum2 = new ArrayList<String>();
			ArrayList<String> sum3 = new ArrayList<String>();


			int i,j;
			boolean flag=false;
			int s=0;
			String helpingas="",helpangos="";

			for (Text val : values) {
			
				help = (val.toString()).split("\\s+");

			
				if (help.length<10){
					sum +=help[5]+","+help[6]+","+help[7];
					
				}
				else{
					
					s++;
					helpingas="";
					helpangos="";
					for (i=0;i<=10;i++){
						helpingas+=help[i]+"\t";
					}
					for(i=11;i<10+thesi;i++){
						helpingas+=help[i]+"\t";
					}
					sum2.add(helpingas);
					for(i=11+thesi;i<21;i++){
						helpangos+=help[i]+"\t";
					}
					sum3.add(helpangos);
				}

			}
				
				
			for(i=0;i<s;i++){
			 	context.write(new Text(sum2.get(i)+sum),new Text(sum3.get(i)));				
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
		job.setJarByClass(ToBinaryOutput.class);
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

