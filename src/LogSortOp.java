import java.util.Vector;

public final class LogSortOp extends LogOp {
	
	Vector<String> sortAttrs = new Vector<>();
	
	@Override
	public void accept(LogOpVisitor visitor) {
		visitor.visit(this);
	}
	
}
