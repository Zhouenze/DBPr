package physicalPlan;
import java.util.Vector;

/*
 * Base class of projection physical operator
 * 
 * @author Enze Zhou ez242
 */
public abstract class PhyProjOp extends PhyOp {

	public boolean selectAll = false;					// Indicated whether this Operator keeps all the columns of its child.
	public Vector<String> projAttrs = new Vector<>();	// Strings that indicates column names that is to be projected.
	
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
			for (int i = 0; i < projAttrs.size(); ++i) {
				schema.put(projAttrs.get(i), i);
			}
		}
	}
}
