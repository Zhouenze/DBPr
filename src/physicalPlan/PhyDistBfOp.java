package physicalPlan;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Vector;

import base.Tuple;

/*
 * Brute force implementation of distinct physical operator
 * 
 * @authors Enze Zhou ez242, Weicheng Yu wy248
 */
public class PhyDistBfOp extends PhyDistOp {
	
	public PriorityQueue<Tuple> heap;   		// Buffer to store all child tuples.
	private Vector<Integer> lastTupleData;		// Recording last tuple to compare with current one.
	
	
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
	 * @override from super class.
	 */
	@Override
	public void reset() {
		child.reset();
		if (hasOrderby)
			lastTupleData = null;
		heap = null;
	}


	/*
	 * Method to build schema of the output of this operator.
	 * @override from super class.
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
		exComparator myComp = new exComparator();
		heap = new PriorityQueue<Tuple>(myComp);
		Tuple temp;
		while((temp = child.getNextTuple()) != null) {
			heap.offer(temp);
		}
	}
	
	
	/*
	 * Customized Comparator to compare tuples based on the sortAttrsIndex
	 */
	private class exComparator implements Comparator<Tuple> {
		
		/*
		 * This function compares two tuples by default priority, because distinct only need an order, with no perticular order.
		 * @param
		 * 		t1 and t2 are two tuples being compared.
		 * @return
		 * 		an integer value determining the result of comparison.
		 */
		public int compare(Tuple t1, Tuple t2) {
			for (int j = 0; j < t1.data.size(); ++j) {
				int ret = t1.data.get(j).compareTo(t2.data.get(j));
				if (ret != 0) return ret;
			}
			return 0;
		}
		
	}
}


