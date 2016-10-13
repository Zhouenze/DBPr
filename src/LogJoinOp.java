
public final class LogJoinOp extends LogOp {
	
	LogScanOp rChild = null;

	@Override
	public void accept(LogOpVisitor visitor) {
		visitor.visit(this);
	}

}
