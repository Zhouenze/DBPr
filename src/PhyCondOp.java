import java.util.Vector;

/*
 * PhyCondOp
 * Operator with a conditions list to filter its output.
 * @superclass Operator
 * 
 * @author Enze Zhou ez242
 */
public abstract class PhyCondOp extends PhyOp {
	
	public Vector<Condition> conditions = new Vector<>();	// Condition list to filter the output of this operator.
	
}
