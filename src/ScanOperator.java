import java.io.OutputStream;

/*
 * Scan Operator
 * Scan a file and output its tuples one by one. Inherited from CondOperator to have a conditions vector that is used to filter the output of this node.
 * @superclass CondOperator
 * 
 * @authors Enze Zhou ez242
 */
public class ScanOperator extends CondOperator {
	
	public String fileName;		// File name that is to be scanned. Full path can be obtained by inferring DBCatalog.
	public String alias;		// Alias of this file. If no alias is provided, it will be the same as the fileName to simplify program.

	/*
	 * Method that return next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * Method that reset output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	/*
	 * Method that dump all the output of this node to a stream.
	 * @override from super class Operator
	 * @param f
	 * 		Stream to be dump to.
	 */
	@Override
	public void dump(OutputStream f) {
		// TODO Auto-generated method stub
		
	}

	/*
	 * Method that print the information of this node.
	 * @override from super class Operator
	 */
	@Override
	public void print() {
		System.out.println("Scan:\t" + alias + " IS " + fileName + " " + schema.toString());
		if (!conditions.isEmpty()) {
			System.out.print("\tScan Conditions: ");
			for (Condition cond : conditions)
				cond.print();
			System.out.println();
		}
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
