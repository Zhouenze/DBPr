import java.io.OutputStream;
import java.util.HashSet;
import java.util.Vector;

/*
 * Distinct operator
 * Build output by deduplicating output of its child Operator.
 * @superclass Operator
 * 
 * @authors Enze Zhou ez242
 */
public class DistinctOperator extends Operator {
	
	/*
	 * Method that return next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	public HashSet<Vector<Integer>> appeared;
    public DistinctOperator() {
		// TODO Auto-generated constructor stub
		appeared = new HashSet<>();
	}
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		Tuple T;
		while ((T=child.getNextTuple()) != null) {
			if (!appeared.contains(T.data)){
				appeared.add(T.data);
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
		// TODO Auto-generated method stub
		appeared.clear();
		child.reset();
	}

	/*
	 * Method that dump all the output of this node to a stream.
	 * @override from super class Operator
	 * @param f
	 * 		Stream to be dump to.
	 */
	@Override
	public void dump(OutputStream f) {
		// TODO Auto-generated method stub
		
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
