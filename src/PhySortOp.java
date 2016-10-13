import java.util.Vector;

public abstract class PhySortOp extends PhyOp {

	public Vector<String> orderAttrs = new Vector<>();
	
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
		schema = child.schema;
	}
}
