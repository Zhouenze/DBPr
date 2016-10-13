package logicalPlan;

/*
 * Base class for all the logical operators
 * 
 * @author Enze Zhou ez242
 */
public abstract class LogOp {
	
	public LogOp child = null;		// Child of this operator. Every node have one child except Join operator, which has two.
	
	/*
	 * Method that is used for visitor pattern.
	 * @param visitor
	 * 		The visitor that is visiting this operator.
	 */
	public void accept(LogOpVisitor visitor) {
		visitor.visit(this);
	}
	
}
