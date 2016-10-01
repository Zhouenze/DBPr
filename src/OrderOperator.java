import java.io.OutputStream;
import java.util.PriorityQueue;
import java.util.Vector;

/*
 * Order Operator
 * Operator that order its child's output in ascending order.
 * @superclass Operator
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public class OrderOperator extends Operator {

	public PriorityQueue<Tuple> heap;   // Buffer to store all child tuples.
	public Vector<String> orderAttrs;	// Strings that indicates column names that is to be ordered by.
										// If not included here, the smaller the index the higher the priority.
	
	/*
	 * Constructor that simply calls super() and initiate orderAttrs.
	 */
	public OrderOperator() {
		super();
		heap = null;
		orderAttrs = new Vector<>();
	}
	
	/*
	 * Method that returns next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		if (heap == null) {
			// call helper method to construct heap of all child tuples
			buildHeap();
		}
		if (heap.isEmpty()) {
			return null;
		}
		
		return heap.poll();
	}

	/*
	 * Method that resets output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		child.reset();
		heap = null;
	}

	/*
	 * Method that prints the information of this node.
	 * @override from super class Operator
	 */
	@Override
	public void print() {
		System.out.println("Orde:\t" + orderAttrs.toString() + " : " + schema.toString());
		child.print();
	}

	/*
	 * Method that builds output schema of this node.
	 * @override from super class Operator
	 */
	@Override
	public void buildSchema() {
		child.buildSchema();
		schema = child.schema;
	}
	
	/*
	 * Method to put all output tuples of child in heap, in order to perform sorting.
	 */
	public void buildHeap(){
		heap = new PriorityQueue<Tuple>();
		// update orderAttrsIndex in Tuple based on orderAttrs
		Tuple.orderAttrsIndex = new Vector<Integer>();
		boolean[] inAttrs = new boolean[schema.size()];
		for (int i = 0; i < inAttrs.length; ++i)
			inAttrs[i] = false;
		for(String attr: orderAttrs) {
			Tuple.orderAttrsIndex.add(schema.get(attr));
			inAttrs[schema.get(attr)] = true;
		}
		for (int i = 0; i < inAttrs.length; ++i)
			if (!inAttrs[i])
				Tuple.orderAttrsIndex.add(i);
		
		Tuple temp;
		while((temp = child.getNextTuple()) != null) {
			heap.offer(temp);
		}
		// could be empty!
	}
}
