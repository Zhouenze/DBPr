
public abstract class PhyScanOp extends PhyCondOp {
	
	public String fileName = "";		// File name that is to be scanned. Full path can be obtained by inferring DBCatalog.
	public String alias = "";		// Alias of this file. If no alias is provided, it will be the same as the fileName to simplify program.

	@Override
	public void accept(PhyOpVisitor visitor) {
		visitor.visit(this);
	}
	
	/*
	 * Method that build output schema of this node.
	 * @override from super class Operator
	 */
	@Override
	public void buildSchema() {
		for (int i = 0; i < DBCatalog.getCatalog().tables.get(fileName).size(); ++i) {
			schema.put(alias + "." + DBCatalog.getCatalog().tables.get(fileName).get(i), i);
		}
	}
}
