
public abstract class PhyJoinOp extends PhyCondOp {
	
	PhyScanOp rChild = null;

	@Override
	public void accept(PhyOpVisitor visitor) {
		visitor.visit(this);
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
