package physicalPlan;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import base.Condition;
import base.DBCatalog;
import base.Tuple;

/*
 * Brute force implementation of scan physical operator
 * Scan a file and output its tuples one by one. Inherited from PhyCondOp to have a conditions vector that is used to filter the output of this node.
 * 
 * @authors Enze Zhou ez242 Weicheng Yu wy248
 */
public class PhyScanBfOp extends PhyScanOp {
	
	public BufferedReader bufferedReader = null;		//keep track of which line in file is being read
	public boolean file_read = false; 					//flag that checks whether the file specified has been open
	
	/*
	 * Method that return next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		if (!file_read) {
			FileReader fileReader;
			try {
				fileReader = new FileReader(DBCatalog.getCatalog().inputPath+"db/data/"+fileName);
				bufferedReader= new BufferedReader(fileReader);
				file_read = true;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
		}
		String line;
		boolean failed = false;
		try {	
			while ((line = bufferedReader.readLine()) != null){
				Tuple retTuple = new Tuple(line);
				for (Condition c: conditions){		
					if (!c.test(retTuple, schema)) {		//if any test fails, set failed and check next ccondition
						failed = true;
						break;
					}
				}
				if (failed){
					failed = false;
					continue;
				}
				return retTuple;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * Method that reset output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		FileReader fileReader;
		try {
			fileReader = new FileReader(DBCatalog.getCatalog().inputPath+"/db/data/"+fileName);		//reopen file
			bufferedReader= new BufferedReader(fileReader);		//also need to set public variables stored in PhyScanBfOp
			file_read = true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

}
