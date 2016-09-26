import java.util.HashMap;

/*
 * Condition
 * Class that represents a condition expression.
 * 
 * @author Enze Zhou ez242
 */
public class Condition {
	
	// Enumerate all the 6 different operators:
	// less than, greater than, less or equal, greater or equal, equal, not equal
	public enum op {
		l, g, le, ge, e, ne
	}
	op operator;
	
	String leftName;	// Name of the left part of this expression. If null, left part is a instant number indicated by left.
	String rightName;	// Name of the right part of this expression. If null, right part is a instant number indicated by right.
	int left;			// Instant left number if leftName is null.
	int right;			// Instant right number if rightName is null.
	
	/*
	 * Constructor that build a condition from a string.
	 * @param condStr
	 * 		String that is going to be turned into a condition.
	 */
	public Condition(String condStr) {
		String [] parts = condStr.trim().split(" ");
		if (parts[1].equals("<=")) {
			operator = op.le;
		} else if (parts[1].equals(">=")) {
			operator = op.ge;
		} else if (parts[1].equals("!=")) {
			operator = op.ne;
		} else if (parts[1].equals("<")) {
			operator = op.l;
		} else if (parts[1].equals(">")) {
			operator = op.g;
		} else if (parts[1].equals("=")) {
			operator = op.e;
		}
		if (Character.isDigit(parts[0].charAt(0))) {
			leftName = null;
			left = Integer.valueOf(parts[0]);
		} else {
			leftName = parts[0];
		}
		if (Character.isDigit(parts[2].charAt(0))) {
			rightName = null;
			right = Integer.valueOf(parts[2]);
		} else {
			rightName = parts[2];
		}
	}
	
	/*
	 * Test whether a tuple satisfies this condition.
	 * Notice: 	this function assumes that the input has all the information needed to judge
	 * 			so both leftName and rightName should be found in schema and tuple.
	 * @param tp
	 * 		Tuple that is being tested.
	 * @param schema
	 * 		Schema of the tuple that is being tested.
	 * @return boolean result whether the input tuple satisfies this condition.
	 */
	public boolean test(Tuple tp, HashMap<String, Integer> schema) {
		int templeft = (leftName == null ? left : tp.data.get(schema.get(leftName)));
		int tempright = (rightName == null ? right : tp.data.get(schema.get(rightName)));
		switch (operator) {
		case l:
			return templeft < tempright;
		case g:
			return templeft > tempright;
		case e:
			return templeft == tempright;
		case le:
			return templeft <= tempright;
		case ge:
			return templeft >= tempright;
		case ne:
			return templeft != tempright;
		default:
			return false;
		}
	}
	
	/*
	 * Method that print this condition.
	 */
	public void print() {
		System.out.print(	(leftName == null ? String.valueOf(left) : leftName) + " " + 
							operator.toString() + " " + 
							(rightName == null ? String.valueOf(right) : rightName) + " : ");
	}
}
