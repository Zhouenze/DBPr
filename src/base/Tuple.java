package base;

import java.util.Vector;


/*
 * Tuple
 * Class that represents a tuple.
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public final class Tuple {
	
	public Vector<Integer> data = null;							// Integers in this Tuple.
	
	/*
	 * Constructor that constructs a tuple with no data
	 */
	public Tuple() {
		data = new Vector<Integer>();
	}
	
	/*
	 * Constructor that fetches data from a tuple string read from a file.
	 * @param str
	 * 		String that build a tuple from
	 */
	public Tuple(String str) {
		data = new Vector<Integer>();
		String [] ints = str.trim().split(",");
		for (String inti : ints)
			data.add(new Integer(inti));
	}
	
	/*
	 * Print this tuple.
	 */
	public void print() {
		for (Integer inti : data)
			System.out.print(inti + " ");
		System.out.println();
	}
}
