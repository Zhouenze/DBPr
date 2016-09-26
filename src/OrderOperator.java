import java.io.OutputStream;
import java.util.Vector;

/*
 * Order Operator
 * Operator that order its child's output in ascending order.
 * @superclass Operator
 * 
 * @authors Enze Zhou ez242
 */
public class OrderOperator extends Operator {

	public Vector<String> orderAttrs;	// Strings that indicates column names that is to be ordered by.
										// If not included here, the smaller the index the higher the priority.
	
	/*
	 * Constructor that simply alls super() and initiate orderAttrs.
	 */
	public OrderOperator() {
		super();
		orderAttrs = new Vector<>();
	}
	
	/*
	 * Method that return next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * Method that reset output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
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
		System.out.println("Orde:\t" + orderAttrs.toString() + " : " + schema.toString());
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
