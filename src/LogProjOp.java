import java.util.Vector;

public final class LogProjOp extends LogOp {
	
	boolean selectAll;
	Vector<String> projAttrs;
	
	public LogProjOp() {
		super();
		selectAll = false;
		projAttrs = new Vector<>();
	}
	
	@Override
	public void accept(LogOpVisitor visitor) {
		visitor.visit(this);
	}

}
