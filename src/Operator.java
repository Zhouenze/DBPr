import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

/*
 * Base class for all the operators
 * Provide some common methods that all operators should implement.
 * 
 * @author Enze Zhou ez242
 */
public abstract class Operator {
	
	public Operator child;					// Every operator has a child. If need more, add in class.
	
	public HashMap<String, Integer> schema;	// Schema of the output of this node. A dictionary whose
											// key is the column name and value is the index of this column.
	/*
	 * Constructor that initialize schema instance.
	 */
	public Operator() { schema = new HashMap<>(); }
	
	/*
	 * Method that return next tuple in the output of this node.
	 * @return next tuple in the output of this node.
	 */
	public abstract Tuple getNextTuple();
	
	/*
	 * Method that reset output of this node to the beginning.
	 */
	public abstract void reset();
	
	/*
	 * Method that dump all the output of this node to a stream.
	 * @param out
	 * 		Stream to be dump to.
	 */
	public void dump(OutputStream out) throws IOException {
		DataOutputStream dataOut = new DataOutputStream((out == null ? System.out : out));
		Tuple temp;
		while ((temp = this.getNextTuple()) != null) {
			for (int i = 0; i < temp.data.size(); ++i)
				dataOut.writeBytes((i == 0 ? temp.data.get(i).toString() : "," + temp.data.get(i)));
			dataOut.write('\n');
		}
	}
	
	/*
	 * Method that print the information of this node.
	 */
	public abstract void print();
	
	/*
	 * Method that build output schema of this node.
	 */
	public abstract void buildSchema();
}
