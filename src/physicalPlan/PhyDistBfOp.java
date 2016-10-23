package physicalPlan;
import java.util.PriorityQueue;
import java.util.Vector;

import base.Tuple;

/*
 * Brute force implementation of distinct physical operator
 * 
 * @authors Enze Zhou ez242, Weicheng Yu wy248
 */
public class PhyDistBfOp extends PhyDistOp {
	
	public PriorityQueue<Tuple> heap;   // Buffer to store all child tuples.
	public boolean hasOrderby = false;
	private Vector<Integer> lastTupleData;
	
	
	/*
	 * Method that returns next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Tuple getNextTuple() {
		//hasOrderby = false; //disallow check
		if (hasOrderby) {
			Tuple T;
			while ((T=child.getNextTuple()) != null) {
				if ((lastTupleData == null) || (!lastTupleData.equals(T.data))){		
					lastTupleData = (Vector<Integer>) T.data.clone();
					return T;
				}
			}
			return null;
		}
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
		if (hasOrderby)
			lastTupleData = null;
		heap = null;
	}


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
		Tuple temp;
		while((temp = child.getNextTuple()) != null) {
			heap.offer(temp);
		}
		// could be empty!
	}
}


