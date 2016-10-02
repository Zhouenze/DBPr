import java.io.OutputStream;
import java.util.HashSet;
import java.util.Vector;

/*
 * Distinct operator
 * Build output by deduplicating output of its child Operator.
 * @superclass Operator
 * 
 * @authors Enze Zhou ez242 Weicheng Yu wy248
 */
public class DistinctOperator extends Operator {
	
	public HashSet<Vector<Integer>> appeared;		//hashset to store all tuple data that have been seen
	
    public DistinctOperator() {
    	super();
		appeared = new HashSet<>();
	}
    
	/*
	 * Method that return next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple T;
		while ((T=child.getNextTuple()) != null) {
			if (!appeared.contains(T.data)){		
				appeared.add(T.data);		//add data to hashset if not already seen
				return T;
			}
		}
		return null;
	}

	/*
	 * Method that reset output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		appeared.clear();		//need to clear variable appeared in this class
		child.reset();
	}

	/*
	 * Method that print the information of this node.
	 * @override from super class Operator
	 */
	@Override
	public void print() {
		System.out.println("Dist:\t" + schema.toString());
		child.print();
	}
	
	/*
	 * Method that build output schema of this node.
	 * @override from super class Operator
	 */
	@Override
	public void buildSchema() {
		child.buildSchema();
		schema = child.schema;
	}
}
