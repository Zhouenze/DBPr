import java.util.Vector;

public abstract class PhySortOp extends PhyOp {

	public Vector<String> orderAttrs;
	
	public PhySortOp() {
		super();
		orderAttrs = new Vector<>(); 
	}
}
