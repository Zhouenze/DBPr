import java.util.Vector;

/*
 * Projection Operator
 * Operator that get some of the columns of its child operator.
 * @superclass Operator
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public class ProjectionOperator extends Operator {
	
	boolean selectAll;			// Indicated whether this Operator keeps all the columns of its child.
	Vector<String> projNames;	// Strings that indicates column names that is to be projected.
	
	/*
	 * Constructor that simply calls super() and initiate its additional variables.
	 */
	public ProjectionOperator() {
		super();
		selectAll = false;
		projNames = new Vector<>();
	}

	/*
	 * Method that returns next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple childnext = child.getNextTuple();
		if (childnext == null || selectAll) {
			return childnext;
		}
		// projection
		// based on childnext's schema and projNames
		Tuple proj = new Tuple();
		for(String attr: projNames) {
			int index = child.schema.get(attr);
			proj.data.add((childnext.data.get(index)));
		}
		return proj;
	}

	/*
	 * Method that resets output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		child.reset();
	}

	/*
	 * Method that prints the information of this node.
	 * @override from super class Operator
	 */
	@Override
	public void print() {
		System.out.println("Proj:\t" + schema.toString());
		child.print();
	}

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
			for (int i = 0; i < projNames.size(); ++i) {
				schema.put(projNames.get(i), i);
			}
		}
	}
}
