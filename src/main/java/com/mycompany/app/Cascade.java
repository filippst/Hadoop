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
import java.text.DecimalFormat;


public class Cascade {

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			String[] temp;
			while (itr.hasMoreTokens()) {
				
		    	temp = (itr.nextToken()).split("\t");
		    	
		    		
		    	context.write(new Text (temp[0]), value);
		    		
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

			//DecimalFormat df = new DecimalFormat("0.0000");
			//temp = df.format(percentage);
			String sum="";
			double prob=0.0;
			for ( Text val : values){
			sum="";
			String[] help = (val.toString()).split("\\s+");
			String[] stats;
			double no=1.0;
			String outp="";
			int i;
			for (i=1;i<11;i++){
				outp+=help[i]+"\t";
			}
			for ( i=11;i<help.length;i++){
				stats = help[i].toString().split(",");
				prob = (Double.parseDouble(stats[1]))*no;
				sum+=String.valueOf(prob)+"\t";
				no=no*(Double.parseDouble(stats[2]));
				
			}

			context.write(key, new Text(outp+sum));
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
		job.setJarByClass(Cascade.class);
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

