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

public class URLtoDomain {

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			String[] temp,help;

			while (itr.hasMoreTokens()) {
				
		    	temp = (itr.nextToken()).split("\t");
		    	
		    	if(temp.length == 5){
		    		
		    		context.write(new Text(temp[0]+"-"+temp[4]+"-"+temp[3]), new Text (temp[1]+"\t"+temp[2]));
		    	}
		    	
		    	if(temp.length == 16){
		    		
		    		    		
		    		help = (temp[6]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
		    		help = (temp[7]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
		    		help = (temp[8]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
		    		help = (temp[9]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
		    		help = (temp[10]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));	
		    		help = (temp[11]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
		    		help = (temp[12]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
		    		help = (temp[13]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
		    		help = (temp[14]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
		    		help = (temp[15]).split(",");
		    		context.write(new Text(temp[0]+"-"+help[0]+"-"+temp[3]), new Text (help[1]));
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
			int s=0,l=0; //s counts the number of times we find the same key,l is times of C
			String sum ="";
			String help = "";
			for (Text val : values) {
				
				if (val.toString().contains("C")){
					l=l+1;
				
					if (l<2){
			 		//help += val.toString()+"\t";
			 		
			 	}
				
			}
			else{ 
				if (!sum.contains(val.toString())) {
					sum += val.toString()+"\t";
				}
			}
			s++;
			
			}
			sum =help+sum;
		
			String[] temp;
			temp = (key.toString()).split("-");
			if ((s>1)&&(l>0)) {
			    context.write(new Text (temp[0]+"\t"+temp[2]), new Text (sum));				
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
		job.setJarByClass(URLtoDomain.class);
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

