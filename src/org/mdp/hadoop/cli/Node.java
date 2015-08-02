package org.mdp.hadoop.cli;

import java.util.*;

import org.apache.hadoop.io.Text;


public class Node {

	public static enum Color {
		WHITE ("WHITE"),
		GRAY ("GRAY"),
		BLACK ("BLACK");
		private String name;
		private Color(String col){
			name=col;
		}
		public String getName(){
			return name;
		}
	};
	private static Map<String, Color> colors = new HashMap<String,Color>();
	static{
		colors.put(Color.WHITE.getName(), Color.WHITE);
		colors.put(Color.GRAY.getName(), Color.GRAY);
		colors.put(Color.BLACK.getName(), Color.BLACK);
	}
	private final int id;
	private int parent = Integer.MAX_VALUE;
	private int distance = Integer.MAX_VALUE;
	private List<Integer> edges = null;
	private Color color = Color.WHITE;

	public Node(int id) {
		this.id = id;
	}

	public Node(String string) {
		//string=5       1##2##4##|2|WHITE|
		String[] line = string.split("\t");
		//line=["5","1##2##4##|2|WHITE|"]
		id = Integer.parseInt(line[0]);
		line = line[1].split("|");
		//line=["1##2##4##" , "2" , "WHITE"]
		edges = new ArrayList<Integer>();
		distance = Integer.parseInt(line[1]);
		color = colors.get(line[2]);
		line = line[0].split("##");
		//line=["1","2,"4"]
		for(int i=0; i<line.length;i++){
			edges.add(Integer.parseInt(line[i]));
		}
	}
	public Text getLine() {
		StringBuilder sb = new StringBuilder();
		sb.append(id);
		sb.append("\t");
		for(Integer edge:edges){
			sb.append(edge);
			sb.append("##");
		}
		sb.append("|");
		sb.append(distance);
		sb.append("|");
		sb.append(color.getName());
		sb.append("|");
		return new Text(sb.toString());
	}

	public int getId(){
		return this.id;
	}

	public int getParent() {
		return this.parent;
	}

	public void setParent(int parent) {
		this.parent = parent;
	}

	public int getDistance(){
		return this.distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public Color getColor(){
		return this.color;
	}

	public void setColor(Color color){
		this.color = color;
	}

	public List<Integer> getEdges(){
		return this.edges;
	}

	public void setEdges(List<Integer> vertices) {
		this.edges = vertices;
	}

	


}