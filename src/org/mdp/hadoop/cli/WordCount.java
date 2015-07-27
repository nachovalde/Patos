package org.mdp.hadoop.cli;

import java.io.IOException;
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
public class WordCount {
	
	/**
	 * Use this with line.split(SPLIT_REGEX) to get fairly nice
	 * word splits.
	 */
	public static String SPLIT_REGEX = "[^\\p{L}'-]+";
	
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
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		
		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the map method that you're goint to write. :)
		 */
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String line = value.toString();
			String[] rawWords = line.split(SPLIT_REGEX);
			for(String rawWord:rawWords) {
				if(!rawWord.isEmpty()){
					word.set(rawWord.toLowerCase());
					output.write(word, one);
				}
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
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the reduce method that you're going to write. :)
		 */
		public void reduce(Text key, Iterator<IntWritable> values,
				Context output) throws IOException, InterruptedException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.getCounter("words", key.toString().substring(0, 1)).increment(1);;
			output.write(key, new IntWritable(sum));
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
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(WordCountMapper.class);
	    job.setCombinerClass(WordCountReducer.class);
	    job.setReducerClass(WordCountReducer.class);
	     
	    job.setJarByClass(WordCount.class);
		job.submit();
	}	
}
