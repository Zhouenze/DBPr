import java.util.Vector;

/*
 * Base class of sort physical operator
 * 
 * @author Enze Zhou ez242
 */
public abstract class PhySortOp extends PhyOp {

	public Vector<String> sortAttrs = new Vector<>();	// Attributes to be sorted by. If not included in this vector, has priority with descending order.
	
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
