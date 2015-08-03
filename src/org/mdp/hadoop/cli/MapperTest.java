package org.mdp.hadoop.cli;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperTest {

	static String[] ta ={ "1	2##5##|0|GRAY|",
			 "2	1##3##4##5##|100000|WHITE|",
			 "3	2##4##|100000|WHITE|",
			 "4	2##3##5##|100000|WHITE|",
			 "5	1##2##4##|100000|WHITE|"};
	public static void main(String[] args) {
		for(String value:ta){
			Node node = new Node(value.toString());
			String out;
			// For each GRAY node, emit each of the edges as a new node (also GRAY)
			if (node.getColor() == Node.Color.GRAY) {
				for (int v : node.getEdges()) {
					Node vnode = new Node(v);
					vnode.setDistance(node.getDistance() + 1);
					vnode.setColor(Node.Color.GRAY);
					out = vnode.getId() + "\t" + vnode.getLine().toString();
					System.out.println(out);
					System.out.println(new Node(out).getId()+"\t"+new Node(out).getLine().toString());
				}
				// We're done with this node now, color it BLACK
				node.setColor(Node.Color.BLACK);
			}

			// No matter what, we emit the input node
			// If the node came into this method GRAY, it will be output as BLACK
			out = node.getId() + "\t" + node.getLine().toString();
			System.out.println(out);
			System.out.println("llave es "+node.getId());
		}
	}
	
	
	/*public void map(IntWritable key, Text value, Context output) throws IOException, InterruptedException {

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
	}*/
}
