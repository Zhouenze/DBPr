
/*
 * Base class of distinct physical operator
 * 
 * @author Enze Zhou ez242
 */
public abstract class PhyDistOp extends PhyOp {
	
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
