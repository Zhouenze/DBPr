package physicalPlan;
import java.util.Vector;

import base.Condition;

/*
 * Base class of conditioned operators, which has a condition list to filter its output.
 * 
 * @author Enze Zhou ez242
 */
public abstract class PhyCondOp extends PhyOp {
	
	public Vector<Condition> conditions = new Vector<>();	// Condition list to filter the output of this operator.
	
}
