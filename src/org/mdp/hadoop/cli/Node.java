package org.mdp.hadoop.cli;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		public boolean darker(Color color) {
			return this.ordinal() > color.ordinal();
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
	static final String VALID_REGEX = "[0-9]+\t([0-9]+##)*\\|[0-9]+\\|((WHITE)|(GRAY)|(BLACK))\\|";
	public Node(final String string) {
		/*if(!string.matches(VALID_REGEX)){
			System.err.println("Error, linea invalida: "+string);
			throw new RuntimeException(){
				public void printStackTrace(PrintStream ps){
					super.printStackTrace(ps);
					ps.println(string);
				}
				@Override
				public void printStackTrace(PrintWriter pw){
					super.printStackTrace(pw);
					pw.println(string);
				}
			};
		}*/
		//string=5       1##2##4##|2|WHITE|
		String[] line = string.split("\t");
		//line=["5","1##2##4##|2|WHITE|"]
		id = Integer.parseInt(line[0]);
		line = line[1].split("\\|");
		//line=["1##2##4##" , "2" , "WHITE"]
		edges = new ArrayList<Integer>();
		distance = Integer.parseInt(line[1]);
		color = colors.get(line[2]);
		line = line[0].split("##");
		//line=["1","2,"4"]
		//System.err.println(line);
		for(int i=0; i<line.length;i++){
			if(line[i].equals("")) continue;
			edges.add(Integer.parseInt(line[i]));
		}
	}
	public Text getLine() {
		StringBuilder sb = new StringBuilder();
		//sb.append(id);
		//sb.append("\t");
		if(edges == null){
		}else{
			for(Integer edge:edges){
				sb.append(edge);
				sb.append("##");
			}
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
