
public final class LogScanOp extends LogOp {

	@Override
	public void accept(LogOpVisitor visitor) {
		visitor.visit(this);
	}
	
}
