import java.util.Vector;

/*
 * Tuple
 * Class that represents a tuple.
 * 
 * @author Enze Zhou ez242
 */
public class Tuple {
	
	public Vector<Integer> data;				// Integers in this Tuple.
	
	/*
	 * Constructor that fetch data from a tuple string read from a file.
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
		for (Integer inti : data) {
			System.out.print(inti);
		}
	}
}
