
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

public class MachineLearningForm {

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			//String[] help = new String[];
			String[] temp;
 			String ola="";
			int i,j;
			int s=1;
			while (itr.hasMoreTokens()) {
				
		    	temp = (itr.nextToken()).split("\t");

		    	// for (i=1;i<=10;i++){
		    	// 	ola +=temp[i]+" ";
		    	// }
		    	ola+=temp[1]+" ";
		    	for (i=11;i<=20;i++){
		    		String[] help = (temp[i]).split(",");
		    		for(j=0;j<3;j++){
		    			ola+=Integer.toString(s)+":"+help[j]+" ";
		    			s++;
		    		}
		    		
		    	}

		    	context.write(new Text(temp[0]),new Text(ola));

		    	
		    	
		    	
		    	
		    	
		    
		    }
		}
	}

	public static class IntSumReducer 
	    extends Reducer<Text,Text,Text,Text> {
		private IntWritable result = new IntWritable();
	
		public void reduce(Text key, Iterable<Text> values,
		            Context context
                    ) throws IOException, InterruptedException {
			
			for (Text val : values) {
				
				context.write(val,new Text(""));	

		}
			

			

			//context.write(values,new Text(""));	
		
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
		job.setJarByClass(MachineLearningForm.class);
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

