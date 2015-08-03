package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is an example Hadoop Map/Reduce application. 
 * 
 * It inputs a map in adjacency list format, and performs a breadth-first search.
 * The input format is
 * ID   EDGES|DISTANCE|COLOR
 * where
 * ID = the unique identifier for a node (assumed to be an int here)
 * EDGES = the list of edges emanating from the node (e.g. 3,8,9,12)
 * DISTANCE = the to be determined distance of the node from the source
 * COLOR = a simple status tracking field to keep track of when we're finished with a node
 * It assumes that the source node (the node from which to start the search) has
 * been marked with distance 0 and color GRAY in the original input.  All other
 * nodes will have input distance Integer.MAX_VALUE and color WHITE.
 */
public class _4GraphSearch{

	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.GraphSearch");

	/**
	 * Nodes that are Color.WHITE or Color.BLACK are emitted, as is. For every
	 * edge of a Color.GRAY node, we emit a new Node with distance incremented by
	 * one. The Color.GRAY node is then colored black and is also emitted.
	 */
	public static class BFSMapper extends Mapper<Object, Text, IntWritable, Text> {

		public void map(IntWritable key, Text value, Context output) throws IOException, InterruptedException {

			Node node = new Node(value.toString());

			// For each GRAY node, emit each of the edges as a new node (also GRAY)
			if (node.getColor() == Node.Color.GRAY) {
				for (int v : node.getEdges()) {
					Node vnode = new Node(v);
					vnode.setDistance(node.getDistance() + 1);
					vnode.setColor(Node.Color.GRAY);
					output.write(new IntWritable(vnode.getId()), vnode.getLine());
				}
				// We're done with this node now, color it BLACK
				node.setColor(Node.Color.BLACK);
			}

			// No matter what, we emit the input node
			// If the node came into this method GRAY, it will be output as BLACK
			output.write(new IntWritable(node.getId()), node.getLine());

		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class BFSReduce extends Reducer<IntWritable, Text, IntWritable, Text> {

		/**
		 * Make a new node which combines all information for this single node id.
		 * The new node should have 
		 * - The full list of edges 
		 * - The minimum distance 
		 * - The darkest Color
		 * @throws InterruptedException 
		 */
		public void reduce(IntWritable key, Iterator<Text> values,
				Context output) throws IOException, InterruptedException {

			List<Integer> edges = null;
			int distance = Integer.MAX_VALUE;
			Node.Color color = Node.Color.WHITE;

			while (values.hasNext()) {
				Text value = values.next();

				Node u = new Node(key.get() + "\t" + value.toString());

				// One (and only one) copy of the node will be the fully expanded
				// version, which includes the edges
				if (u.getEdges().size() > 0) {
					edges = u.getEdges();
				}

				// Save the minimum distance
				if (u.getDistance() < distance) {
					distance = u.getDistance();
				}

				// Save the darkest color
				if (u.getColor().darker(color)) {
					color = u.getColor();
				}

			}

			Node n = new Node(key.get());
			n.setDistance(distance);
			n.setEdges(edges);
			n.setColor(color);
			output.write(key, new Text(n.getLine()));
		}
	}


	private static boolean keepGoing(int iterationCount) {
		if(iterationCount >= 1) {
			return false;
		}

		return true;
	}

	public static void main(String[] args) throws Exception {
		
		int iterationCount = 0;
		while (keepGoing(iterationCount)) {
			
			String input;
			if (iterationCount == 0)
				input = "/uhadoop/ivalderrama/bfs/input-graph";
			else
				input = "/uhadoop/ivalderrama/bfs/output-graph-" + iterationCount;

			String output = "/uhadoop/ivalderrama/bfs/output-graph-" + (iterationCount + 1);
			
			Job job = Job.getInstance(new Configuration());
		    FileInputFormat.setInputPaths(job, new Path(input));
		    FileOutputFormat.setOutputPath(job, new Path(output));
		    
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setMapperClass(BFSMapper.class);
		    job.setReducerClass(BFSReduce.class);
		     
		    job.setJarByClass(_3AdjacencyListCreator.class);
		    job.waitForCompletion(true);
		}
	}	

}
