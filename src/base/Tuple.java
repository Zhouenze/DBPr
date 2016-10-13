package base;
import java.util.Vector;

/*
 * Tuple
 * Class that represents a tuple.
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public class Tuple implements Comparable<Tuple>{
	
	public static Vector<Integer> orderAttrsIndex = null; 		// Index of attrs needed to be sorted, in descending priority
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
	
	/*
	 * Method that defines how tuples are sorted
	 * according to attributes, their indices and priorities.
	 * @param tp
	 *       Tuple with which we are comparing this tuple
	 */
	@Override
	public int compareTo(Tuple tp) {		
		for(int index: orderAttrsIndex) {
			int result = this.data.get(index).compareTo(tp.data.get(index));
			if(result != 0) {
				return result;
			}
		}
		return 0;
	}
}
