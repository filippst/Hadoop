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

public class Users {

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			//Pattern p = Pattern.compile("((\\d{4}-\\d{2})-\\d{2}) \\d{2}:\\d{2}:\\d{2}\\t+(\\d+\\t+http\\S+)*$");
			//Matcher m;
			String[] temp;
			while (itr.hasMoreTokens()) {
				//m = p.matcher(itr.nextToken());
		    	temp = (itr.nextToken()).split("\t");
		    	if(temp.length == 4){
		    		//word.set(m.group(1));
		    		context.write(new Text(temp[0]), new Text (temp[1]+"\t"+temp[2]+"\t"+temp[3]));
		    	}
		    }
		}
	}

	public static class IntSumReducer 
	    extends Reducer<Text,Text,Text,Text> {
		private IntWritable result = new IntWritable();
	
		//public void reduce(Text key, Iterable<IntWritable> values,
		public void reduce(Text key, Text values, 
                    Context context
                    ) throws IOException, InterruptedException {
			// int sum = 0;
			// for (IntWritable val : values) {
			// 	sum += val.get();
			// }
			// result.set(sum);
			// context.write(key, result);

			context.write(key, values);

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
		job.setJarByClass(Users.class);
		job.setJobName("DayCounter");
		    
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
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

