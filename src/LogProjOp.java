import java.util.Vector;

/*
 * Projection logical operator
 * 
 * @author Enze Zhou ez242
 */
public final class LogProjOp extends LogOp {
	
	boolean selectAll = false;					// Whether this operator select all the attributes.
	
	Vector<String> projAttrs = new Vector<>();	// If doesn't select all, what are the columns to select.

}
