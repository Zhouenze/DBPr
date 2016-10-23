package physicalPlan;
import java.io.DataOutputStream;
import base.TupleWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

import base.Tuple;

/*
 * Base class for all the physical operators
 * Provide some common methods that all operators should implement.
 * 
 * @author Enze Zhou ez242
 */
public abstract class PhyOp {
	
	public PhyOp child = null;									// Every operator has a child. If need more, add in class.
	
	public HashMap<String, Integer> schema = new HashMap<>();	// Schema of the output of this node. A dictionary whose
																// key is the column name and value is the index of this column.
	
	/*
	 * Method that return next tuple in the output of this node.
	 * @return next tuple in the output of this node.
	 */
	public abstract Tuple getNextTuple();
	
	/*
	 * Method that reset output of this node to the beginning.
	 */
	public abstract void reset();
	
	/*
	 * Method that dump all the output of this node to a stream.
	 * @param out
	 * 		Stream to be dump to.
	 */
	public final void dumpReadable(OutputStream out) throws IOException {
		DataOutputStream dataOut = new DataOutputStream((out == null ? System.out : out));
		Tuple temp;
		while ((temp = this.getNextTuple()) != null) {
			for (int i = 0; i < temp.data.size(); ++i)
				dataOut.writeBytes((i == 0 ? temp.data.get(i).toString() : "," + temp.data.get(i)));
			dataOut.write('\n');
		}
	}
	
	public void dump(OutputStream out) throws IOException {
		TupleWriter TW = new TupleWriter(out);
		Tuple temp;
		while ((temp = this.getNextTuple()) != null) {
			TW.setNextTuple(temp);
		}
		if (!TW.bufferEmpty())
			TW.fillFlush();
	}
	
	
	/*
	 * Method that is used for visitor pattern.
	 * @param visitor
	 * 		The visitor that is visiting this operator.
	 */
	public void accept(PhyOpVisitor visitor) {
		visitor.visit(this);
	}
	
	/*
	 * Method that build output schema of this node.
	 */
	public abstract void buildSchema();
}
