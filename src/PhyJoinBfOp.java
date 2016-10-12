
/*
 * Join Operator
 * Operator that joins the outputs of its two children, both are Scan Operators inferred by Operator pointer.
 * Inherited from PhyCondOp to have a conditions vector that is used to filter the output tuples of this node.
 * @superclass PhyCondOp
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public class PhyJoinBfOp extends PhyCondOp {
	
	public PhyOp rChild;		// The right child of this operator.
	public Tuple left;			// The left tuple now. This need to be an element of class
								// because it should keep between different calls to getNextTuple().
	boolean end;				// denote whether this node has already be fully got.
	
	/*
	 * Constructor simply calls super and initialize new elements.
	 */
	public PhyJoinBfOp() {
		super();
		rChild = null;
		left = null;
		end = false;
	}

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

	/*
	 * Method that prints the information of this node.
	 * @override from super class Operator
	 */
	@Override
	public void print() {
		System.out.println("Join:\t" + schema.toString());
		child.print();
		rChild.print();
		if (!conditions.isEmpty()) {
			System.out.print("\tJoin conditions: ");
			for (Condition cond : conditions)
				cond.print();
			System.out.println();
		}
	}

	/*
	 * Method that builds output schema of this node.
	 * @override from super class Operator
	 */
	@Override
	public void buildSchema() {
		child.buildSchema();
		rChild.buildSchema();
		
		for (String name : child.schema.keySet()) {
			schema.put(name, child.schema.get(name));
		}
		
		// Because the two children are concatenated together, the index of the right
		// child columns in output should add the size of the number of columns of the left child.
		int add = child.schema.size();
		for (String name : rChild.schema.keySet()) {
			schema.put(name, rChild.schema.get(name) + add);
		}
	}
}
