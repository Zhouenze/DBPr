import java.util.Vector;

public final class LogProjOp extends LogOp {
	
	boolean selectAll = false;
	Vector<String> projAttrs = new Vector<>();
	
	@Override
	public void accept(LogOpVisitor visitor) {
		visitor.visit(this);
	}

}
