import java.io.OutputStream;

/*
 * Join Operator
 * Operator that joins the outputs of its two children, both are Scan Operators inferred by Operator pointer.
 * Inherited from CondOperator to have a conditions vector that is used to filter the output tuples of this node.
 * @superclass CondOperator
 * 
 * @authors Enze Zhou ez242
 */
public class JoinOperator extends CondOperator {
	
	public Operator rChild;		// The right child of this operator.

	/*
	 * Method that return next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		while(child.getNextTuple() != null) {
			Tuple left = child.getNextTuple();
			while(rChild.getNextTuple() != null) {
				Tuple right = rChild.getNextTuple();
				
				// concatenate left & right
				Tuple join = new Tuple();
				for(int i: left.data) {
					join.data.add(i);
				}
				for(int j: right.data) {
					join.data.add(j);
				}
				
				// test
				if(conditions == null) { // if cross product
					return join;
				} else { // test through all conditions
					for(int i = 0; i < conditions.size(); i++) {
						Condition c = conditions.get(i);
						if(!c.test(join, schema)) {
							break;
						}
						// do something to join according to the condition
						
						if(i == conditions.size() - 1) {
							return join;
						}
					}
					// failed condition test
					continue;
				}
				
			}
			// reset inner child
			rChild.reset();
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
	 * Method that build output schema of this node.
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
