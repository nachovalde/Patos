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

// input: /uhadoop/ivalderrama/out2/paircompress
// output: /uhadoop/ivalderrama/bfs

/**
 * Java class to run a remote Hadoop word count job.
 * 
 * Contains the main method, an inner Reducer class 
 * and an inner Mapper class.
 * 
 * @author Aidan
 */
public class Bacon3AdjacencyListCreator {
	
	/**
	 * Use this with line.split(SPLIT_REGEX) to get fairly nice
	 * word splits.
	 */
	public static String SPLIT_REGEX = "\t";
	public static class AdjacencyListMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

		private IntWritable star1 = new IntWritable();
		private IntWritable star2 = new IntWritable();
		@Override
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String line = value.toString();
			String[] raw = line.split(SPLIT_REGEX);
			if(raw[0].equals(raw[1])) return;
			star1.set(Integer.parseInt(raw[0]));
			star2.set(Integer.parseInt(raw[1]));
			output.write(star1, star2);
			output.write(star2, star1);
		}
	}
	public static class AdjacencyListReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
		/**
		 * @throws InterruptedException 
		 * @Override
		 * 
		 * This is the reduce method that you're going to write. :)
		 */
		static String regular = "|"+Integer.MAX_VALUE+"|WHITE|";
		static String searched = "0";
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context output) throws IOException, InterruptedException {
			Iterator<IntWritable> ite = values.iterator();
			StringBuilder sb = new StringBuilder();
			while (ite.hasNext()) {
				sb.append(ite.next().toString());
				sb.append("##");
			}
			sb.append(key.toString().equals(searched)?"|"+0+"|GRAY|":regular);
			output.write(key, new Text(sb.toString()));
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

	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    
	    job.setMapperClass(AdjacencyListMapper.class);
	    job.setReducerClass(AdjacencyListReducer.class);
	     
	    job.setJarByClass(Bacon3AdjacencyListCreator.class);
	    job.waitForCompletion(true);	
	}	
}
