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

public class WasItSeen {

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			String[] temp,help;
			int pos=0;
			int i ;
			while (itr.hasMoreTokens()) {
				
		    	temp = (itr.nextToken()).split("\t");
		    	
		    	if(temp.length == 16){

		    		context.write(new Text(temp[0]+"\t"+temp[3]),new Text (temp[1]+"\t"+temp[2]+"\t"+temp[4]+"\t"+temp[5]+"\t"+temp[6]+"\t"+temp[7]+"\t"+temp[8]+"\t"+temp[9]+"\t"+temp[10]+"\t"+temp[11]+"\t"+temp[12]+"\t"+temp[13]+"\t"+temp[14]+"\t"+temp[15]));
		    	}	
		    	else if (temp.length == 5)
		    	{
		    			context.write(new Text(temp[0]+"\t"+temp[3]), new Text (temp[4]));

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
			
			String sum ="",help = "";
			String[] last;
			int max=-3,i,j;

			for (Text val : values) {

				if (val.toString().contains("Q")){
					
				
					
			 		help += val.toString()+"\t";
			 		
			 	}
			 	else {
			 		sum += val.toString()+"\t";
			 	}
			}
			sum =help+sum;
			last  = (sum).split("\t");
			    
		    
			for (i=14;i<(last.length);i++){
				
				for (j=4;j<14;j++){
					
					
					if (last[j].equals(last[i])){
						
						if (j>max){
							max = j;
						}
						break;
						
					}
				}
			}
			

			sum = sum+Integer.toString(max+2);


			context.write(new Text(key),new Text(sum));				
						

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
		job.setJarByClass(CountClicks.class);
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

