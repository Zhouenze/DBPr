import java.util.HashMap;
import java.util.Vector;
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

public class LogPlan implements SelectVisitor, FromItemVisitor {
	
	private LogSortOp sort = null;
	private LogProjOp proj = null;
	private LogOp dataRoot = null;
	
	String query = "";
	Vector<Condition> conditions = new Vector<>();
	HashMap<String, String> aliasDict = new HashMap<>();
	Vector<String> naiveJoinOrder = new Vector<>();
	
	LogOp root = null;
	
	public LogPlan(Select select) {
		query = select.toString();
		select.getSelectBody().accept(this);
	}

	@Override
	public void visit(PlainSelect plainSelect) {
		
		LogOp temp = null;
		
		// If there is a distinct, parse it.
		if (plainSelect.getDistinct() != null) {
			root = temp = new LogDistOp();
		}
		
		// If there is an order, parse it.
		if (plainSelect.getOrderByElements() != null) {
			if (temp != null) {
				temp.child = sort = new LogSortOp();
				temp = temp.child;
			} else {
				root = temp = sort = new LogSortOp();
			}
			
			// Get all the order attributes and save them to order operator.
			Iterator orderIterator = plainSelect.getOrderByElements().iterator();
			while (orderIterator.hasNext()) {
				String str = orderIterator.next().toString();
				sort.sortAttrs.add(str);
			}
		}
		
		// Make a projection operator which is mandatory.
		if (temp != null) {
			temp.child = proj = new LogProjOp();
			temp = temp.child;
		} else {
			root = temp = proj = new LogProjOp();
		}
		
		// Get all the projection target. If *, set selectAll. If no *, insert all the others to projection operator.
		proj.selectAll = plainSelect.getSelectItems().get(0).toString().equals("*");
		if (!proj.selectAll) {
			Iterator projIterator = plainSelect.getSelectItems().iterator();
			while (projIterator.hasNext()) {
				String str = projIterator.next().toString();
				proj.projAttrs.add(str);
			}
		}
		
		// Build scan operators and join operators.
		if (plainSelect.getJoins() == null) {
			
			// If there is only one scan.
			LogScanOp tempScan = new LogScanOp();
			String fileName = plainSelect.getFromItem().toString().split(" ")[0];
			String alias = (plainSelect.getFromItem().getAlias() == null ? fileName : plainSelect.getFromItem().getAlias());
			aliasDict.put(alias, fileName);
			naiveJoinOrder.add(alias);
			proj.child = dataRoot = tempScan;
			
		} else {
			
			// If there are many tables to join.
			// Build the leftmost two scan nodes and join node.
			LogJoinOp joinRoot = new LogJoinOp();
			LogJoinOp tempJoin;
			LogScanOp tempScan = new LogScanOp();
			String fileName = plainSelect.getFromItem().toString().split(" ")[0];
			String alias = (plainSelect.getFromItem().getAlias() == null ? fileName : plainSelect.getFromItem().getAlias());
			aliasDict.put(alias, fileName);
			naiveJoinOrder.add(alias);
			joinRoot.child = tempScan;
			
			Iterator joinsIt = plainSelect.getJoins().iterator();
			Join join = (Join) joinsIt.next();
			tempScan = new LogScanOp();
			fileName = join.getRightItem().toString().split(" ")[0];
			alias = (join.getRightItem().getAlias() == null ? fileName : join.getRightItem().getAlias());
			aliasDict.put(alias, fileName);
			naiveJoinOrder.add(alias);
			joinRoot.rChild = tempScan;
			
			// Then build all the others into a left-deep join tree.
			while (joinsIt.hasNext()) {
				join = (Join) joinsIt.next();
				tempJoin = new LogJoinOp();
				tempJoin.child = joinRoot;
				tempScan = new LogScanOp();
				fileName = join.getRightItem().toString().split(" ")[0];
				alias = (join.getRightItem().getAlias() == null ? fileName : join.getRightItem().getAlias());
				aliasDict.put(alias, fileName);
				naiveJoinOrder.add(alias);
				tempJoin.rChild = tempScan;
				joinRoot = tempJoin;
			}
			proj.child = dataRoot = joinRoot;
		}
		
		// If there are where clauses
		if (plainSelect.getWhere() != null) {
			String [] whereClauses;
			whereClauses = plainSelect.getWhere().toString().split("AND");
			for (int i = 0; i < whereClauses.length; ++i) {
				conditions.add(new Condition(whereClauses[i]));
			}
		}
	}

	@Override
	public void visit(Union Union) {}
	
	@Override
	public void visit(Table Table) {}
	
	@Override
	public void visit(SubSelect SubSelect) {}
	
	@Override
	public void visit(SubJoin SubJoin) {}
	
}
