package physicalPlan;

import java.io.IOException;

import base.Condition;
import base.DBCatalog;
import base.Tuple;
import base.TupleReader;


/**
 * Brute force implementation of scan physical operator
 * Scan a file and output its tuples one by one. Inherited from PhyCondOp to have a conditions vector that is used to filter the output of this node.
 * 
 * @authors Enze Zhou ez242 Weicheng Yu wy248
 */
public final class PhyScanBfOp extends PhyScanOp {
	
	
	private TupleReader tupleReader;		// Reader for binary file.
	private boolean read = false;
	
	
	/**
	 * Method that return next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		if (!read){
			try {
				String data = DBCatalog.getCatalog().inputPath.contains("/") ? "db/data/" : "db\\data\\";
				tupleReader = new TupleReader(DBCatalog.getCatalog().inputPath+data+fileName);
				read = true;
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		boolean failed = false;
		try {
			Tuple temp = null;
			while ((temp = tupleReader.getNextTuple()) != null){
				for (Condition c: conditions){		
					if (!c.test(temp, schema)) {		//if any test fails, set failed and check next ccondition
						failed = true;
						break;
					}
				}
				if (failed){
					failed = false;
					continue;
				}
				
				return temp;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Method that reset output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset()  {
		try {
			if (tupleReader != null)
				tupleReader.reset();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Get string representation of this operator.
	 * @override from superclass PhyScanOp
	 * @see java.lang.Object#toString()
	 * @return
	 * 		string representation of this operator.
	 */
	@Override
	public String toString() {
		return String.format("TableScan[%s]", fileName);
	}

}

