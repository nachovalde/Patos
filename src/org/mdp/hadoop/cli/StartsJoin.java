package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//uhadoop@cluster   -   HADcc5212$oop
/**
 * Java class to run a remote Hadoop word count job.
 * 
 * Contains the main method, an inner Reducer class 
 * and an inner Mapper class.
 * 
 * @author Aidan
 */
public class StartsJoin {
	
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
	public static class StartsJoinMapper extends Mapper<Object, Text, Text, Text>{

		//private final IntWritable one = new IntWritable(1);
		private Text start = new Text();
		private Text movie = new Text();

		
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
			String[] rawWords = line.split(SPLIT_REGEX);
			if(rawWords[4].compareTo("THEATRICAL_MOVIE") == 0){
				start.set(rawWords[0]);
				movie.set(rawWords[1] + rawWords[2] + rawWords[3]);
				output.write(movie, start);
			}
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
	public static class StartsJoinReducer extends Reducer<Text, Text, Text, IntWritable>{
		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the reduce method that you're going to write. :)
		 */
		String bacon = "Bacon, Kevin";
		String min = "\0";
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			ArrayList<Text> starts = new ArrayList<Text>();
			Iterator<Text> ite = values.iterator();
			while(ite.hasNext()){
				starts.add(ite.next());
			}
			Collections.sort(starts);
			for (int i = 0; i < starts.size(); i++) {
				for (int j = 0; j < starts.size(); j++) {
					if(j==i) continue;
					String s1 = starts.get(i).toString();
					String s2 = starts.get(j).toString();
					if(s1.compareTo(s2) == 0) continue;
					if(s1.compareTo(bacon)==0){
						s1 = min;
						s2 = s2+"#1";
					}
					else if(s2.compareTo(bacon) == 0){
						s2 = min+"#1";
					}
					output.write(new Text(s1+ "##" + s2), new IntWritable(1));
				}
			}
			
		}
	}

	/**
	 * Main method that sets up and runs the job
	 * 
	 * @param args First argument is input, second is output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: StartsCount <in> <out>");
			System.exit(2);
		}
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];
		
		Job job = Job.getInstance(new Configuration());
	     
	    FileInputFormat.setInputPaths(job, new Path(inputLocation));
	    FileOutputFormat.setOutputPath(job, new Path(outputLocation));
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setMapperClass(StartsJoinMapper.class);
	    job.setReducerClass(StartsJoinReducer.class);
	     
	    job.setJarByClass(StartsJoin.class);
	    job.waitForCompletion(true);
	}	
}