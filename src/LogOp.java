
public abstract class LogOp {
	
	LogOp child = null;
	
	public abstract void accept(LogOpVisitor visitor);
	
}
