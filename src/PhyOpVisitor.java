
public interface PhyOpVisitor {
	void visit(PhyDistOp PhyDistOp);
	
	void visit(PhyJoinOp PhyJoinOp);
	
	void visit(PhyProjOp PhyProjOp);
	
	void visit(PhyScanOp PhyScanOp);
	
	void visit(PhySortOp PhySortOp);
}
