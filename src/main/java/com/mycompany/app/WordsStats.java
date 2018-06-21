//input URLtoDomain kai to input ????

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

public class WordsStats {

	public static class TokenizerMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
   
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			String[] temp,help;
			String[] help6,help7,help8,help9,help10,help11,help12,help13,help14,help15;
			
			int pos=0;
			int i ;
			while (itr.hasMoreTokens()) {
				
		    	temp = (itr.nextToken()).split("\t");
		    	
		    	if(temp.length == 16){

		    		
		    		help6 = temp[6].split(",");
		            help7 = temp[7].split(",");
		    		help8 = temp[8].split(",");
		    		help9 = temp[9].split(",");
		    		help10 = temp[10].split(",");
		    		help11 = temp[11].split(",");
		    		help12 = temp[12].split(",");
		    		help13 = temp[13].split(",");
		    		help14 = temp[14].split(",");
		    		help15 = temp[15].split(",");
		    			
		    		context.write(new Text(temp[0]+"-"+temp[3]), new Text (temp[5]+"\t"+help6[1]+"\t"+help7[1]+"\t"+help8[1]+"\t"+help9[1]+"\t"+help10[1]+"\t"+help11[1]+"\t"+help12[1]+"\t"+help13[1]+"\t"+help14[1]+"\t"+help15[1]));
		    		
		    	}	
		    	else if (temp.length == 3)
		    	{
		    			context.write(new Text(temp[0]+"-"+temp[1]), new Text (temp[2]));

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
			String help = "";
			int max=-1,i,j;
			String[] last,words;
			String[] len;
				
			for (Text val : values) {
				
				len  = (val.toString()).split("\\s+");

				if (len.length>3){
					
				
					
			 		help += val.toString()+"\t";
			 		
			 	}
			 	else {
			 		sum += val.toString()+"\t";
			 	}
			}

				sum =help+sum;
				last  = (sum).split("\\s+");
				words =(last[0]).split(",");
				for (i=12;i<last.length;i++){
					for(j=0;j<words.length;j++){
						context.write(new Text(last[i]),new Text(words[j]));
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
		job.setJarByClass(WordsStats.class);
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

