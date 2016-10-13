
public abstract class PhyDistOp extends PhyOp {
	
	@Override
	public void accept(PhyOpVisitor visitor) {
		visitor.visit(this);
	}
	
	/*
	 * Method that build output schema of this node.
	 * @override from super class Operator
	 */
	@Override
	public void buildSchema() {
		child.buildSchema();
		schema = child.schema;
	}
}
