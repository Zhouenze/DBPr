
public abstract class LogOp {
	
	LogOp child;
	
	public abstract void accept(LogOpVisitor visitor);
	
}
