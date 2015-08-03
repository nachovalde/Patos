package org.mdp.hadoop.cli;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Bacon2PairsCompressor {
	static final String SPLIT_REGEX = "\t";
	static final String BACON = "Bacon, Kevin (I)";
	private String dict, searched;
	public Bacon2PairsCompressor(String dictPath, String searched){
		dict = dictPath;
		this.searched = searched;
	}

	public int compress(String src, String dest) throws IOException{
		System.err.println("Starting compression");
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int i=1;
		map.put(searched, 0);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(src))));
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create( new Path(dest))));
		BufferedWriter d = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(dict))));
		String[] L;
		System.err.println("Reading input and writing the compressed output");
		int iter = 0;
		for(String line = br.readLine(); line!=null; line=br.readLine()){
			L = line.split(SPLIT_REGEX);
			Integer val = map.get(L[0]);
			if(val==null){
				map.put(L[0], i);
				val = i++;
			}
			out.write(val+"\t");
			val = map.get(L[1]);
			if(val==null){
				map.put(L[1], i);
				val = i++;
			}
			out.write(val+"\n");
			if(iter++%100000==0) System.err.println(iter+" lines processed");
		}
		br.close();
		out.close();
		iter=0;
		System.err.println("Writing the dictionary");
		for(String val:map.keySet()){
			d.write(map.get(val)+"\t"+val+"\n");
			if(iter++%100000==0) System.err.println(iter+" lines processed");
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
		Bacon2PairsCompressor ac = new Bacon2PairsCompressor(dictLocation, Bacon2PairsCompressor.BACON);
		ac.compress(inputLocation, outputLocation);
	}

}
