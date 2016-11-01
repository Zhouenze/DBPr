package logicalPlan;

/*
 * Distinct logical operator
 * 
 * @author Enze Zhou ez242
 */
public final class LogDistOp extends LogOp {
	public boolean hasOrderby = false;		//added boolean for sorting, this can make already sorted input run faster
}
