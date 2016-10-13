package physicalPlan;
import java.util.PriorityQueue;
import java.util.Vector;

import base.Tuple;

/*
 * Brute force implementation of sort operator
 * Operator that order its child's output in ascending order.
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public class PhySortBfOp extends PhySortOp {

	public PriorityQueue<Tuple> heap = null;   // Buffer to store all child tuples.
	
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
	 * Method to put all output tuples of child in heap, in order to perform sorting.
	 */
	public void buildHeap(){
		heap = new PriorityQueue<Tuple>();
		// update orderAttrsIndex in Tuple based on orderAttrs
		Tuple.orderAttrsIndex = new Vector<Integer>();
		boolean[] inAttrs = new boolean[schema.size()];
		for (int i = 0; i < inAttrs.length; ++i)
			inAttrs[i] = false;
		for(String attr: sortAttrs) {
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
