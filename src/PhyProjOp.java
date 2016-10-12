import java.util.Vector;

public abstract class PhyProjOp extends PhyOp {

	boolean selectAll;			// Indicated whether this Operator keeps all the columns of its child.
	Vector<String> projNames;	// Strings that indicates column names that is to be projected.
	
	public PhyProjOp() {
		super();
		projNames = new Vector<>();
	}
	
}
