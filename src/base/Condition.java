package base;
import java.util.HashMap;

/**
 * Condition
 * Class that represents a condition expression.
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public final class Condition {
	
	// Enumerate all the 6 different operators:
	// less than, greater than, less or equal, greater or equal, equal, not equal
	public enum op {
		l("<"),
		g(">"),
		le("<="),
		ge(">="),
		e("="),
		ne("<>");
		
		private final String opStr;			// String representation of an operator
		
		/**
		 * Constructor of a enumerate element
		 * @param opStr
		 * 		String representation of this element.
		 */
		private op(final String opStr) {
			this.opStr = opStr;
		}
		
		/**
		 * Get string representation of this operator.
		 * @see java.lang.Object#toString()
		 * @return
		 * 		string representation of this operator.
		 */
		@Override
		public String toString() {
			return this.opStr;
		}
	}
	public op operator;				// operator of a condition
	
	public String leftName = "";	// Name of the left part of this expression. If null, left part is an integer number indicated by left.
	public String rightName = "";	// Name of the right part of this expression. If null, right part is an integer number indicated by right.
	public int left = 0;			// Instant left number if leftName is null.
	public int right = 0;			// Instant right number if rightName is null.
	
	/**
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
		} else if (parts[1].equals("<>")) {
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
		
		// Flip the condition to a more favorable direction.
		if (leftName == null && rightName != null)
			flip();
	}
	
	/**
	 * Test whether this condition is attr = attr.
	 * @return true if it is, false otherwise.
	 */
	public boolean isEquality() {
		if (leftName == null || rightName == null || operator != op.e) {
			return false;
		}
		return true;
	}
	
	/**
	 * Test whether this condition is attr OP val, where OP is not <> (or != or ne).
	 * Note "Usable" here is slightly different from that defined in the instructions,
	 * in terms of excluding the form of attr = attr, which is tested separately.
	 * @return true if it is, false otherwise.
	 */
	public boolean isUsable() {
		// Condition 閮借flip杩囷紝鎵�浠ヤ竴瀹氫笉鏄� 宸al OP 鍙砤ttr ///////////////////
		if (leftName == null || rightName != null || operator == op.ne) {
			return false;
		}
		return true;
	}
	
	/**
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
	
	/**
	 * Method that change a condition to it's string representation.
	 * @return
	 * 		The string representation of this condition.
	 */
	@Override
	public String toString() {
		return 	(leftName == null ? String.valueOf(left) : leftName) + " " + 
				operator.toString() + " " + 
				(rightName == null ? String.valueOf(right) : rightName);
	}
	
	/*
	 * This function change the direction of this condition while keeping the meaning of it.
	 * This assumption is used in index scan.
	 */
	public void flip() {
		String temp = leftName;
		leftName = rightName;
		rightName = temp;
		int temp2 = left;
		left = right;
		right = temp2;
		switch (operator) {
		case l:
			operator = op.g;
			break;
		case g:
			operator = op.l;
			break;
		case le:
			operator = op.ge;
			break;
		case ge:
			operator = op.le;
			break;
		default:
			break;
		}
	}
}
