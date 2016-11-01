package physicalPlan;

/*
 * Base class of distinct physical operator
 * 
 * @author Enze Zhou ez242
 */
public abstract class PhyDistOp extends PhyOp {
	
	// Whether there is an order by operator under this distinct operator. If so, the distinction can be accelerated.
	public boolean hasOrderby = false;
	
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
