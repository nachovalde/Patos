package org.mdp.hadoop.cli;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class ResultParser {
	static String path = "C:/Users/Joaquin/Documents/res.tsv";//Prime example of good software engineering
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		int[] distances = new int[9];
		for(String line = br.readLine(); line != null; line = br.readLine()){
			int distance = Integer.parseInt(line.split("\t")[1]);
			if(distance == Integer.MAX_VALUE) distance = 8;
			distances[distance]++;
		}
		br.close();
		System.out.println(Arrays.toString(distances));
	}
}
