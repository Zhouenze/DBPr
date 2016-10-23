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
public class PhyScanBfOp extends PhyScanOp {
	
	
	public TupleReader tupleReader;
	public boolean read = false;
	/*
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
				// TODO Auto-generated catch block
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * Method that reset output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset()  {
		try {
			if (tupleReader != null)
				tupleReader.reset();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
