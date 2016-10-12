
public final class LogDistOp extends LogOp {

	@Override
	public void accept(LogOpVisitor visitor) {
		visitor.visit(this);
	}

}
