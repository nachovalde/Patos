package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Java class to run a remote Hadoop word count job.
 * 
 * Contains the main method, an inner Reducer class 
 * and an inner Mapper class.
 * 
 * @author Aidan
 */
public class StartsCount {
	
	/**
	 * Use this with line.split(SPLIT_REGEX) to get fairly nice
	 * word splits.
	 */
	public static String SPLIT_REGEX = "\t";
	
	/**
	 * This is the Mapper Class. This sends key-value pairs to different machines
	 * based on the key.
	 * 
	 * Remember that the generic is Mapper<InputKey, InputValue, MapKey, MapValue>
	 * 
	 * InputKey we don't care about (a LongWritable will be passed as the input
	 * file offset, but we don't care; we can also set as Object)
	 * 
	 * InputKey will be Text: a line of the file
	 * 
	 * MapKey will be Text: a word from the file
	 * 
	 * MapValue will be IntWritable: a count: emit 1 for each occurrence of the word
	 * 
	 * @author Aidan
	 *
	 */
	public static class StartsCountMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final IntWritable one = new IntWritable(1);
		private Text start = new Text();
		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the map method that you're goint to write. :)
		 */
		@Override
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String line = value.toString();
			String[] raw = line.split(SPLIT_REGEX);
			start.set(raw[0]);
			output.write(start, one);

		}
	}
	public static class AdjacencyListMapper extends Mapper<Object, Text, Text, Text>{

		private Text star1 = new Text();
		private Text star2 = new Text();
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String line = value.toString();
			String[] raw = line.split(SPLIT_REGEX);
			if(raw[0].equals(raw[1])) return;
			star1.set(raw[0]);
			star2.set(raw[1]);
			output.write(star1, star2);
			output.write(star2, star1);
		}
	}
	/**
	 * This is the Reducer Class.
	 * 
	 * This collects sets of key-value pairs with the same key on one machine. 
	 * 
	 * Remember that the generic is Reducer<MapKey, MapValue, OutputKey, OutputValue>
	 * 
	 * MapKey will be Text: a word from the file
	 * 
	 * MapValue will be IntWritable: a count: emit 1 for each occurrence of the word
	 * 
	 * OutputKey will be Text: the same word
	 * 
	 * OutputValue will be IntWritable: the final count
	 * 
	 * @author Aidan
	 *
	 */
	public static class StartsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the reduce method that you're going to write. :)
		 */
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context output) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> ite = values.iterator();
			while (ite.hasNext()) {
				sum += ite.next().get();
			}
			output.write(key, new IntWritable(sum));
		}
	}
	public static class AdjacencyListReducer extends Reducer<Text, Text, Text, Text> {
		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the reduce method that you're going to write. :)
		 */
		static String regular = "|"+Integer.MAX_VALUE+"|WHITE|";
		static String bacon = "Bacon, Kevin";
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			Iterator<Text> ite = values.iterator();
			StringBuilder sb = new StringBuilder();
			while (ite.hasNext()) {
				sb.append(ite.next().toString());
				sb.append("##");
			}
			sb.append(key.toString().equals(bacon)?"|"+0+"|GRAY|":regular);
			output.write(key, new Text(sb.toString()));
		}
	}

	/**
	 * Main method that sets up and runs the job
	 * 
	 * @param args First argument is input, second is output
	 * @throws Exception
	 */
	//String baconregex = "\\p{#}*#"+iter;
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];
		
		Job job = Job.getInstance(new Configuration());
	     
	    FileInputFormat.setInputPaths(job, new Path(inputLocation));
	    FileOutputFormat.setOutputPath(job, new Path(outputLocation));
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setMapperClass(AdjacencyListMapper.class);
	    job.setReducerClass(AdjacencyListReducer.class);
	     
	    job.setJarByClass(StartsCount.class);
	    job.waitForCompletion(true);	
	}	
}