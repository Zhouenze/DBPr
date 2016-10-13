package logicalPlan;
import java.util.Vector;

/*
 * Sort logical operator
 * 
 * @author Enze Zhou ez242
 */
public final class LogSortOp extends LogOp {
	
	public Vector<String> sortAttrs = new Vector<>();		// The column names that is being sorted.
	
}
