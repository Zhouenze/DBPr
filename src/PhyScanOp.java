
public abstract class PhyScanOp extends PhyCondOp {
	
	public String fileName;		// File name that is to be scanned. Full path can be obtained by inferring DBCatalog.
	public String alias;		// Alias of this file. If no alias is provided, it will be the same as the fileName to simplify program.

}
