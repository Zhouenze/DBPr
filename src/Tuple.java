import java.util.Vector;

/*
 * Tuple
 * Class that represents a tuple.
 * 
 * @author Enze Zhou ez242
 */
public class Tuple implements Comparable<Tuple>{
	
	public static Vector<Integer> orderAttrsIndex = null; 	// Index of attrs needed to be sorted, in descending priority
	public Vector<Integer> data;							// Integers in this Tuple.
	
	/*
	 * Constructor that construct a tuple with no data
	 */
	public Tuple() {
		data = new Vector<Integer>();
	}
	
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
		for (Integer inti : data)
			System.out.print(inti + " ");
		System.out.println();
	}
	
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
