package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
public class _1PairGenerator {
	
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
	public static class MovieMapper extends Mapper<Object, Text, Text, Text>{
		private Text movie = new Text();
		private Text actor = new Text();
		private static final String tmovie = "THEATRICAL_MOVIE";
		
		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the map method that you're going to write. :)
		 */
		@Override
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String line = value.toString();
			String[] actorEntry = line.split(SPLIT_REGEX);
			if(actorEntry[4].equals(tmovie)){
				movie.set((actorEntry[1]+actorEntry[2]+actorEntry[3]).toLowerCase());
				actor.set(actorEntry[0]);
				output.write(movie, actor);
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
	
	public static class MovieReducer extends Reducer<Text, Text, Text, NullWritable> {
		Text out = new Text();
		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the reduce method that you're going to write. :)
		 */
		@Override
		public void reduce(Text movie, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			ArrayList<String> actors = new ArrayList<String>();
			for(Text actor:values){
				actors.add(actor.toString());
			}
			Collections.sort(actors,String.CASE_INSENSITIVE_ORDER);
			for(int i=0; i<actors.size(); i++){
				for (int j = i+1; j < actors.size(); j++) {
					out.set(actors.get(i)+"\t"+actors.get(j));
					output.write(out,NullWritable.get());
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
	    
	    job.setMapperClass(MovieMapper.class);
	    job.setReducerClass(MovieReducer.class);
	     
	    job.setJarByClass(_1PairGenerator.class);
		job.waitForCompletion(true);
	}	
}
