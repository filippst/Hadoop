//input to arxiko

package com.mycompany.app;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.*;

public class OnlyClicks {

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			boolean flag=false;
			String[] temp;
			int i;
			while (itr.hasMoreTokens()) {
				flag=false;
		    	temp = (itr.nextToken()).split("\t");
		    	for (i=1;i<11;i++){
		    		if (temp[i].equals("1")){
		    			flag = true;
		    			break;
		    		}

		    	}
		    	if (flag==true){
		    		context.write(new Text(temp[0]),value);
		    	}
		    	}
		    
		}
	}

	public static class IntSumReducer 
	    extends Reducer<Text,Text,Text,Text> {
		private IntWritable result = new IntWritable();
	
		//public void reduce(Text key, Iterable<IntWritable> values,
		public void reduce(Text key, Iterable<Text> values, 
                    Context context
                    ) throws IOException, InterruptedException {

			for( Text vals : values){

			context.write(vals,new Text(""));
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
		job.setJarByClass(OnlyClicks.class);
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

