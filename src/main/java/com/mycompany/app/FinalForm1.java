//2 inputs from ToBinaryOutput and AllStats
//tautoxrona tha petaksw ta clicks afou den ta thelw pia

package com.mycompany.app;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.*;

public class FinalForm1 {
public static int thesi = 1;

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
		/*if (otherArgs.length < 2) {
			System.err.println("Usage: com.apache.hadoop.MapReduce.DateCounter <in> [<in>...] <out>");
		    System.exit(2);
		}*/
		    
		// Path tempDir1 = new Path("Test1");
		// Path tempDir2 = new Path("Test2");
		// Path tempDir3 = new Path("Test3");
		// Path tempDir4 = new Path("Test4");
		// Path tempDir5= new Path("Test5");
		// Path tempDir6 = new Path("Test6");
		// Path tempDir7 = new Path("Test7");
		// Path tempDir8 = new Path("Test8");
		// Path tempDir9 = new Path("Test9");
		// Path tempDir10 = new Path("Test10");
		
		// Path stats = new Path("CountTimesSlate");

		Path tempDir1 = new Path("/home/mofista/Desktop/Results-Filippos/Test1");
		Path tempDir2 = new Path("/home/mofista/Desktop/Results-Filippos/Test2");
		Path tempDir3 = new Path("/home/mofista/Desktop/Results-Filippos/Test3");
		Path tempDir4 = new Path("/home/mofista/Desktop/Results-Filippos/Test4");
		Path tempDir5= new Path("/home/mofista/Desktop/Results-Filippos/Test5");
		Path tempDir6 = new Path("/home/mofista/Desktop/Results-Filippos/Test6");
		Path tempDir7 = new Path("/home/mofista/Desktop/Results-Filippos/Test7");
		Path tempDir8 = new Path("/home/mofista/Desktop/Results-Filippos/Test8");
		Path tempDir9 = new Path("/home/mofista/Desktop/Results-Filippos/Test9");
		Path tempDir10 = new Path("/home/mofista/Desktop/Results-Filippos/Test10");
		
		Path stats = new Path("/home/mofista/Desktop/Results-Filippos/CountTimesSlate");

		Job job1 = new Job(conf, "DayCounter");
		job1.setJarByClass(FinalForm1.class);
		job1.setJobName("DayCounter");
		    
		job1.setMapperClass(TokenizerMapper.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, stats);
		 FileInputFormat.addInputPath(job1, new Path ("/home/mofista/Desktop/Results-Filippos/ToBinaryOutput"));
	//	FileInputFormat.addInputPath(job1, new Path ("ToBinaryOutput"));

		
		FileOutputFormat.setOutputPath(job1,
		tempDir1);
		job1.waitForCompletion(true); 

		Job job2 = new Job(conf, "DayCounter");
		job2.setJarByClass(FinalForm2.class);
		job2.setJobName("DayCounter");
		    
		job2.setMapperClass(FinalForm2.TokenizerMapper.class);
		job2.setReducerClass(FinalForm2.IntSumReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, tempDir1);
		FileInputFormat.addInputPath(job2, stats);
		
		FileOutputFormat.setOutputPath(job2,
		tempDir2);
		job2.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir1, true);

		Job job3 = new Job(conf, "DayCounter");
		job3.setJarByClass(FinalForm2.class);
		job3.setJobName("DayCounter");
		    
		job3.setMapperClass(FinalForm3.TokenizerMapper.class);
		job3.setReducerClass(FinalForm3.IntSumReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job3, tempDir2);
		FileInputFormat.addInputPath(job3, stats);
		
		FileOutputFormat.setOutputPath(job3,
		tempDir3);
		job3.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir2, true);

		Job job4 = new Job(conf, "DayCounter");
		job4.setJarByClass(FinalForm4.class);
		job4.setJobName("DayCounter");
		    
		job4.setMapperClass(FinalForm4.TokenizerMapper.class);
		job4.setReducerClass(FinalForm4.IntSumReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job4, tempDir3);
		FileInputFormat.addInputPath(job4, stats);
		
		FileOutputFormat.setOutputPath(job4,
		tempDir4);
		job4.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir3, true);

		Job job5 = new Job(conf, "DayCounter");
		job5.setJarByClass(FinalForm5.class);
		job5.setJobName("DayCounter");
		    
		job5.setMapperClass(FinalForm5.TokenizerMapper.class);
		job5.setReducerClass(FinalForm5.IntSumReducer.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job5, tempDir4);
		FileInputFormat.addInputPath(job5, stats);
		
		FileOutputFormat.setOutputPath(job5,
		tempDir5);
		job5.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir4, true);

		Job job6 = new Job(conf, "DayCounter");
		job6.setJarByClass(FinalForm6.class);
		job6.setJobName("DayCounter");
		    
		job6.setMapperClass(FinalForm6.TokenizerMapper.class);
		job6.setReducerClass(FinalForm6.IntSumReducer.class);
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job6, tempDir5);
		FileInputFormat.addInputPath(job6, stats);
		
		FileOutputFormat.setOutputPath(job6,
		tempDir6);
		job6.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir5, true);

		Job job7 = new Job(conf, "DayCounter");
		job7.setJarByClass(FinalForm7.class);
		job7.setJobName("DayCounter");
		    
		job7.setMapperClass(FinalForm7.TokenizerMapper.class);
		job7.setReducerClass(FinalForm7.IntSumReducer.class);
		job7.setOutputKeyClass(Text.class);
		job7.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job7, tempDir6);
		FileInputFormat.addInputPath(job7, stats);
		
		FileOutputFormat.setOutputPath(job7,
		tempDir7);
		job7.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir6, true);

		Job job8 = new Job(conf, "DayCounter");
		job8.setJarByClass(FinalForm8.class);
		job8.setJobName("DayCounter");
		    
		job8.setMapperClass(FinalForm8.TokenizerMapper.class);
		job8.setReducerClass(FinalForm8.IntSumReducer.class);
		job8.setOutputKeyClass(Text.class);
		job8.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job8, tempDir7);
		FileInputFormat.addInputPath(job8, stats);
		
		FileOutputFormat.setOutputPath(job8,
		tempDir8);
		job8.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir7, true);

		Job job9 = new Job(conf, "DayCounter");
		job9.setJarByClass(FinalForm9.class);
		job9.setJobName("DayCounter");
		    
		job9.setMapperClass(FinalForm9.TokenizerMapper.class);
		job9.setReducerClass(FinalForm9.IntSumReducer.class);
		job9.setOutputKeyClass(Text.class);
		job9.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job9, tempDir8);
		FileInputFormat.addInputPath(job9, stats);
		
		FileOutputFormat.setOutputPath(job9,
		tempDir9);
		job9.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir8, true);

		Job job10 = new Job(conf, "DayCounter");
		job10.setJarByClass(FinalForm10.class);
		job10.setJobName("DayCounter");
		    
		job10.setMapperClass(FinalForm10.TokenizerMapper.class);
		job10.setReducerClass(FinalForm10.IntSumReducer.class);
		job10.setOutputKeyClass(Text.class);
		job10.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job10, tempDir9);
		FileInputFormat.addInputPath(job10, stats);
		
		FileOutputFormat.setOutputPath(job10,
		tempDir10);
		job10.waitForCompletion(true); 

		FileSystem.get(conf).delete(tempDir9, true);



		System.exit(0);


	}
	
	
}

