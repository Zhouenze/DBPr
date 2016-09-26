import java.util.Vector;

/*
 * CondOperator
 * Operator with a conditions list to filter its output.
 * @superclass Operator
 * 
 * @author Enze Zhou ez242
 */
public abstract class CondOperator extends Operator {
	
	public Vector<Condition> conditions;	// Condition list to filter the output of this operator.
	
	/*
	 * Constructor that simply calls super() and initiates condition list.
	 */
	public CondOperator() {
		super();
		conditions = new Vector<>();
	}
}
