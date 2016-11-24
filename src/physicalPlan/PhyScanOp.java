package physicalPlan;
import base.DBCatalog;

/*
 * Base class of scan physical operator
 * 
 * @author Enze Zhou ez242
 */
public abstract class PhyScanOp extends PhyCondOp {
	
	public String fileName = "";		// File name that is to be scanned. Full path can be obtained by inferring DBCatalog.
	public String alias = "";			// Alias of this file. If no alias is provided, it will be the same as the fileName to simplify program.
	
	/*
	 * Method that build output schema of this node.
	 * @override from super class Operator
	 */
	@Override
	public void buildSchema() {
		for (int i = 0; i < DBCatalog.getCatalog().tables.get(fileName).attrs.size(); ++i) {
			schema.put(alias + "." + DBCatalog.getCatalog().tables.get(fileName).attrs.get(i).name, i);
		}
	}
}
