
/*
 * Join Operator
 * Operator that joins the outputs of its two children, both are Scan Operators inferred by Operator pointer.
 * Inherited from PhyCondOp to have a conditions vector that is used to filter the output tuples of this node.
 * @superclass PhyCondOp
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public class PhyJoinBfOp extends PhyJoinOp {
	
	public Tuple left = null;			// The left tuple now. This need to be an element of class
								// because it should keep between different calls to getNextTuple().
	boolean end = false;				// denote whether this node has already be fully got.

	/*
	 * Method that returns next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		// if this join has ended, return null, prevent restart.
		if (end)
			return null;
		
		// if first call, left is still null. give it a new value as start.
		if (left == null)
			left = child.getNextTuple();
		
		// if left meets null, it means nothing can be found.
		while(left != null) {
			Tuple right;
			while((right = rChild.getNextTuple()) != null) {				
				// concatenate left & right
				Tuple join = new Tuple();
				for(int i: left.data) {
					join.data.add(i);
				}
				for(int j: right.data) {
					join.data.add(j);
				}
				
				// conditions always exist.
				for(int i = 0; i < conditions.size(); i++) {
					Condition c = conditions.get(i);
					if(!c.test(join, schema)) {
						break;
					}
					
					if(i == conditions.size() - 1) {
						return join;
					}
				}
				
			}
			// reset inner child. Only when this time will need to get next left child.
			rChild.reset();
			left = child.getNextTuple();
		}
		end = true;
		return null;
	}

	/*
	 * Method that resets output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		end = false;
		left = null;
		child.reset();
		rChild.reset();
	}

}
