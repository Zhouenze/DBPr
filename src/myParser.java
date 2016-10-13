import java.util.Iterator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

/*
 * Parser that build a tree from select query.
 * Notice: 	Every select statement should have a separate parser because by definition
 * 			the variables can not be shared by different clauses (for safety reasons don't do so,
 * 			though it should be OK).
 * 
 * @author Enze Zhou ez242
 */
public class myParser implements SelectVisitor, FromItemVisitor {
	
	PhyOp root = null;				// Point to root node of parse result of this select statement.
	PhySortBfOp order = null;		// Point to order node if exist.
	PhyProjBfOp proj = null;	// Point to projection node. Must exist.
	PhyCondOp dataRoot = null;		// Point to highest join node or the only scan node available.
	PhyOp temp = null;				// Used by parse process.
	
	/*
	 * API of this class. Build operator tree from a select clause.
	 * @param select
	 * 		select clause that is being parsed.
	 * @return root node of the parse result.
	 */
	public PhyOp parseSelect (Select select) {
		select.getSelectBody().accept(this);
		return root;
	}

	/*
	 * Main workload of parser. Visit a plainselect clause and build result.
	 * @param plainSelect
	 * 		Clause that is being parsed.
	 */
	@Override
	public void visit(PlainSelect plainSelect) {
		
		// If there is a dinsinct, parse it.
		if (plainSelect.getDistinct() != null) {
			root = temp = new PhyDistBfOp();
		}
		
		// If there is an orderby, parse it.
		if (plainSelect.getOrderByElements() != null) {
			if (temp != null) {
				temp.child = order = new PhySortBfOp();
				temp = temp.child;
			} else {
				root = temp = order = new PhySortBfOp();
			}
			
			// Get all the orderby attributes and save them to order operator.
			Iterator orderIterator = plainSelect.getOrderByElements().iterator();
			while (orderIterator.hasNext()) {
				String str = orderIterator.next().toString();
				order.orderAttrs.add(str);
			}
		}
		
		// Make a projection operator which is mandatory.
		if (temp != null) {
			temp.child = proj = new PhyProjBfOp();
			temp = temp.child;
		} else {
			root = temp = proj = new PhyProjBfOp();
		}
		
		// Get all the projection target. If *, set selectAll. If no *, insert all the others to projection operator.
		proj.selectAll = plainSelect.getSelectItems().get(0).toString().equals("*");
		if (!proj.selectAll) {
			Iterator projIterator = plainSelect.getSelectItems().iterator();
			while (projIterator.hasNext()) {
				String str = projIterator.next().toString();
				proj.projNames.add(str);
			}
		}
		
		// Build scan operators and join operators.
		if (plainSelect.getJoins() == null) {
			
			// If there is only one scan.
			PhyScanBfOp tempScan = new PhyScanBfOp();
			tempScan.fileName = plainSelect.getFromItem().toString().split(" ")[0];
			tempScan.alias = (plainSelect.getFromItem().getAlias() == null ? tempScan.fileName : plainSelect.getFromItem().getAlias());
			proj.child = dataRoot = tempScan;
		} else {
			
			// If there are many talbes to join.
			// Build the leftmost two scan nodes and join node.
			PhyJoinBfOp joinRoot = new PhyJoinBfOp();
			PhyJoinBfOp tempJoin;
			PhyScanBfOp tempScan = new PhyScanBfOp();
			tempScan.fileName = plainSelect.getFromItem().toString().split(" ")[0];
			tempScan.alias = (plainSelect.getFromItem().getAlias() == null ? tempScan.fileName : plainSelect.getFromItem().getAlias());
			joinRoot.child = tempScan;
			Iterator joinsIt = plainSelect.getJoins().iterator();
			Join join = (Join) joinsIt.next();
			tempScan = new PhyScanBfOp();
			tempScan.fileName = join.getRightItem().toString().split(" ")[0];
			tempScan.alias = (join.getRightItem().getAlias() == null ? tempScan.fileName : join.getRightItem().getAlias());
			joinRoot.rChild = tempScan;
			
			// Then build all the others into a left-deep join tree.
			while (joinsIt.hasNext()) {
				join = (Join) joinsIt.next();
				tempJoin = new PhyJoinBfOp();
				tempJoin.child = joinRoot;
				tempScan = new PhyScanBfOp();
				tempScan.fileName = join.getRightItem().toString().split(" ")[0];
				tempScan.alias = (join.getRightItem().getAlias() == null ? tempScan.fileName : join.getRightItem().getAlias());
				tempJoin.rChild = tempScan;
				joinRoot = tempJoin;
			}
			proj.child = dataRoot = joinRoot;
		}
		
		// Build schema of every node.
		root.buildSchema();
		
		// If there are where clauses
		if (plainSelect.getWhere() != null) {
			String [] whereClauses;
			whereClauses = plainSelect.getWhere().toString().split("AND");
			for (int i = 0; i < whereClauses.length; ++i) {
				
				// Analyze an condition.
				Condition tempCond = new Condition(whereClauses[i]);
				
				// And attach it to a scan or join node which is as low as possible.
				// But Instant number expressions should be attached to dataRoot because 1 > 3 should invalidate all the tuples.
				if (tempCond.leftName == null && tempCond.rightName == null)
					dataRoot.conditions.add(tempCond);
				else
					if (!conditionDis(dataRoot, tempCond))
						// Dispenser failed.
						System.err.println("Condition undispensed!");
			}
		}

	}
	
	/*
	 * Condition dispenser
	 * @param op
	 * 		Check whether this condition should be attached to this node.
	 * @param cond
	 * 		The condition that is being attached.
	 * @return whether the condition is successfully attached.
	 */
	boolean conditionDis(PhyOp op, Condition cond) {
		if (op == null)
			return false;
		
		// If successfully attached to a child node, shortcut return.
		if (conditionDis(op.child, cond))
			return true;
		if (op instanceof PhyJoinBfOp && conditionDis(((PhyJoinBfOp)op).rChild, cond))
			return true;
		
		// If not enough information available in this node, return false.
		if (cond.leftName != null && op.schema.get(cond.leftName) == null)
			return false;
		if (cond.rightName != null && op.schema.get(cond.rightName) == null)
			return false;
		
		// Attach the condition to this node.
		PhyCondOp condOp = (PhyCondOp)op;
		condOp.conditions.add(cond);
		return true;
	}
	
	/*
	 * Required by interface but no implementation provided because it's unnecessary.
	 */
	@Override
	public void visit(Table tableName) {}

	/*
	 * Required by interface but no implementation provided because it's unnecessary.
	 */
	@Override
	public void visit(SubJoin arg0) {}

	/*
	 * Required by interface but no implementation provided because it's unnecessary.
	 */
	@Override
	public void visit(Union arg0) {}

	/*
	 * Required by interface but no implementation provided because it's unnecessary.
	 */
	@Override
	public void visit(SubSelect arg0) {}
}
