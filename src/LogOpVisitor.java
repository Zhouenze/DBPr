
public interface LogOpVisitor {
	void visit(LogDistOp logDistOp);
	
	void visit(LogJoinOp logJoinOp);
	
	void visit(LogProjOp logProjOp);
	
	void visit(LogScanOp logScanOp);
	
	void visit(LogSortOp logSortOp);
}
