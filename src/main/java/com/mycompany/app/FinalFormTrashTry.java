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

public class FinalFormTrashTry {
public static int thesi = 2;

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
		    	
		    		
		    		if (temp.length==9){
		    		context.write(new Text(temp[0]),value);
		    		}
		    		else{
		    			
		    			context.write(new Text (temp[16]),value);
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
			
			String sum ="";
			String[] help = new String[30];
			String[] help1 = new String[30];
			ArrayList<Text> babouska = new ArrayList<Text>();
			
			String sum2 ;
			String sum3 ;


			int i,j;
			boolean flag=false;
			int s=0;


			for (Text val : values) {
			
				help = (val.toString()).split("\\s+");
			
				if (help.length<10){
					//System.out.println("4 epilegw esena "+help[4]);
					sum +=help[4]+"\t";
					
				}
				else{
					if (help[17]=="4548421") {
						System.out.println(" coca cola "+help[0]);
						System.out.println(" coca cola "+help[0]);
						System.out.println(" coca cola "+help[0]);
						System.out.println(" coca cola "+help[0]);
						System.out.println(" coca cola "+help[0]);

					}
				}
			
				babouska.add(val);	

			}
			
			Iterable<Text> babyfive = babouska;

			for (Text val2 : babyfive) {
			
				help1 = (val2.toString()).split("\\s+");
				
			
				if (help1.length<10){
					
					
				}
				else{
					
				//	System.out.println("exoume "+help1[16]);
					
					sum2="";
					sum3="";
					for (i=0;i<=9;i++){
						sum2+=help1[i]+"\t";
					}
					for(i=16;i<16;i++){
						sum2+=help1[i]+"\t";
					}
					for(i=17;i<26;i++){
						sum3+=help1[i]+"\t";
					}

					context.write(new Text(""),new Text(sum2+sum+sum3));
				
				}

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

