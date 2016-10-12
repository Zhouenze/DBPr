
public final class LogJoinOp extends LogOp {
	
	LogScanOp rChild;

	@Override
	public void accept(LogOpVisitor visitor) {
		visitor.visit(this);
	}

}
