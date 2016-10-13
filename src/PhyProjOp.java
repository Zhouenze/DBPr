import java.util.Vector;

public abstract class PhyProjOp extends PhyOp {

	boolean selectAll = false;			// Indicated whether this Operator keeps all the columns of its child.
	Vector<String> projNames = new Vector<>();	// Strings that indicates column names that is to be projected.
	
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
		if (selectAll) {
			for (String name : child.schema.keySet()) {
				schema.put(name, child.schema.get(name));
			}
		} else {
			for (int i = 0; i < projNames.size(); ++i) {
				schema.put(projNames.get(i), i);
			}
		}
	}
}
