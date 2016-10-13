
/*
 * Brute force implementation of projection physical operator
 * Operator that get some of the columns of its child operator.
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public class PhyProjBfOp extends PhyProjOp {

	/*
	 * Method that returns next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple childnext = child.getNextTuple();
		if (childnext == null || selectAll) {
			return childnext;
		}
		// projection
		// based on childnext's schema and projNames
		Tuple proj = new Tuple();
		for(String attr: projAttrs) {
			int index = child.schema.get(attr);
			proj.data.add((childnext.data.get(index)));
		}
		return proj;
	}

	/*
	 * Method that resets output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		child.reset();
	}


}
