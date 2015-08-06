package org.mdp.hadoop.cli;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// input: /uhadoop/ivalderrama/out1/part-r-00007
// output: any file

public class Bacon5DecompressorAndParser {
	static final String SPLIT_REGEX = "\t";
	private static final int TICKS = 100000;
	private Map<Integer, String> map;
	public Bacon5DecompressorAndParser(String dictPath) throws IOException{
		map = new HashMap<Integer, String>();
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(dictPath))));
		System.err.println("Reading dictionary");
		int iter = 0;
		String line;
		String[] entry;
		for(line = br.readLine();line!=null; line=br.readLine()){
			entry = line.split(SPLIT_REGEX);
			map.put(Integer.parseInt(entry[0]), entry[1]);
			if(++iter%TICKS == 0) System.err.println("Iterations: "+iter);
		}
	}

	public int decompress(String src, String dest) throws IOException{
		System.err.println("Starting decompression");
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(src))));
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create( new Path(dest))));
		String[] L;
		System.err.println("Reading input and writing the compressed output");
		int iter = 0;
		for(String line = br.readLine(); line!=null; line=br.readLine()){
			L = line.split(SPLIT_REGEX);
			String val = map.get(Integer.parseInt(L[0]));
			if(val==null){
				System.err.println(val + " is not in the dictionary");;
			}
			out.write(val+"\t");
			L = L[1].split("\\|");
			out.write(L[1]);
			out.write("\n");
			if(iter++%TICKS==0) System.err.println(iter+" lines processed");
		}
		br.close();
		out.close();
		iter=0;
		System.err.println("Finished decompressing");
		return iter;
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err.println("Usage: "+Bacon5DecompressorAndParser.class.getName()+" <in> <dict> <out>");
			System.exit(2);
		}
		String inputLocation = args[0];
		String dictLocation = args[1];
		String outputLocation = args[2];
		Bacon5DecompressorAndParser ac = new Bacon5DecompressorAndParser(dictLocation);
		ac.decompress(inputLocation, outputLocation);
	}

}
