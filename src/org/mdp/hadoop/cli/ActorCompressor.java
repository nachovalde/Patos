package org.mdp.hadoop.cli;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.omg.PortableInterceptor.ACTIVE;

public class ActorCompressor {
	static final String SPLIT_REGEX = "\t";
	static final String BACON = "Bacon, Kevin (I)";
	private String dict, searched;
	public ActorCompressor(String dictPath, String searched){
		dict = dictPath;
		this.searched = searched;
	}

	public int compress(String src, String dest) throws IOException{
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int i=1;
		map.put(searched, 0);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(src))));
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(dest)));
		BufferedWriter d = new BufferedWriter(new FileWriter(new File(dict)));
		String[] L;
		for(String line = br.readLine(); line!=null; line=br.readLine()){
			System.out.println(line);
			L = line.split(SPLIT_REGEX);
			Integer val = map.get(L[0]);
			if(val==null){
				map.put(L[0], i);
				val = i++;
			}
			out.write(val+"\t");
			L = line.split(SPLIT_REGEX);
			val = map.get(L[1]);
			if(val==null){
				map.put(L[1], i);
				val = i++;
			}
			out.write(val+"\n");
		}
		br.close();
		out.close();
		for(String val:map.keySet()){
			d.write(map.get(val)+"\t"+val+"\n");
		}
		d.close();
		return i;
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err.println("Usage: WordCount <in> <dict> <out>");
			System.exit(2);
		}
		String inputLocation = args[0];
		String dictLocation = args[1];
		String outputLocation = args[2];
		ActorCompressor ac = new ActorCompressor(dictLocation, ActorCompressor.BACON);
		ac.compress(inputLocation, outputLocation);
	}

}
