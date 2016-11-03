package physicalPlan;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Vector;

import base.Tuple;

/*
 * Brute force implementation of sort operator
 * Operator that order its child's output in ascending order.
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public final class PhySortBfOp extends PhySortOp {

	private PriorityQueue<Tuple> heap = null;   // Buffer to store all child tuples.
	public Vector<Integer> sortAttrsIndex = null;

	
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
	private void buildHeap(){
		if(sortAttrsIndex == null) { // ? could only be called once?
			this.buildAttrsIndex();
		}
		exComparator myComp = new exComparator();
		heap = new PriorityQueue<Tuple>(myComp);
		// update orderAttrsIndex in Tuple based on orderAttrs
		
		Tuple temp;
		while((temp = child.getNextTuple()) != null) {
			heap.offer(temp);
		}
		// could be empty!
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
			if (sortAttrsIndex==null) {
				for (int j = 0; j < t1.data.size(); ++j) {
					int ret = t1.data.get(j).compareTo(t2.data.get(j));
					if (ret != 0) return ret;
				}
				return 0;
			}
			for(int index: sortAttrsIndex) {
				int result = t1.data.get(index).compareTo(t2.data.get(index));
				if(result != 0) {
					return result;
				}
			}
			return 0;
		}
	}
	
	/*
	 * Method to update sortAttrsIndex based on sortAttrs
	 */
	private void buildAttrsIndex() {
		sortAttrsIndex = new Vector<Integer>();
		boolean[] inAttrs = new boolean[schema.size()];
		for(int i = 0; i < inAttrs.length; i++) {
			inAttrs[i] = false;
		}
		for(String attr: sortAttrs) {
			sortAttrsIndex.add(schema.get(attr));
			inAttrs[schema.get(attr)] = true;
		}
		for(int i = 0; i < inAttrs.length; i++) {
			if(!inAttrs[i]) {
				sortAttrsIndex.add(i);
			}
		}
	}
}