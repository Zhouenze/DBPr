package base;

/*
 * A level of abstration to help with writing records onto files
 * 
 * @author Weicheng Yu wy248
 */
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Vector;

public class TupleWriter {
	//private String fileName;
	private FileChannel FC;
	private ByteBuffer BB;
	private int tupleIndex;
	private int tupleSize;
	private boolean sizeSet;
	private int tupleCurrentCounts;
	
	@SuppressWarnings("resource")
	public TupleWriter(String filename) throws IOException {
		sizeSet = false;
		FC = new FileOutputStream(filename).getChannel();
		BB = ByteBuffer.allocate(4096);
		setTupleInfo();
		BB.putInt(4, 0);
	}
	
	public TupleWriter(OutputStream out) throws IOException {
		sizeSet = false;
		FC = ((FileOutputStream)out).getChannel();
		BB = ByteBuffer.allocate(4096);
		setTupleInfo();
		BB.putInt(4, 0);
	}
	
	/*
	 * Method that is used to set meta data
	 */
	public void setTupleInfo() throws IOException {
		//first two ints for meta data
		tupleIndex = 8;
		tupleCurrentCounts = 0;
	}
	
	/*
	 * Method that is used to check whether current buffer has no records
	 */
	public boolean bufferEmpty() {
		return tupleCurrentCounts == 0;
	}
	
	/*
	 * @param size: size of a record
	 * Method that is used to set meta data tuple size
	 */
	public void setTupleSize(int size) {
		sizeSet = true;
		tupleSize = size;
		BB.putInt(0, size);
	}
	
	/*
	 * @param toAdd: a tuple to be added to the buffer
	 * Method that is used to prepare for adding tuples
	 */
	public void addTuple(Tuple toAdd) {
		for (int i = 0; i < tupleSize; ++i) {
			BB.putInt(tupleIndex, toAdd.data.elementAt(i));
			tupleIndex += 4;
		}
		BB.putInt(4, ++tupleCurrentCounts);
	}
	
	/*
	 * @param toAdd: a tuple to be added to the buffer
	 * Method that is used to prepare for adding tuples
	 */
	public void setNextTuple(Tuple toAdd) throws IOException{
		if (!sizeSet) {
			setTupleSize(toAdd.data.size());
		}
		
	    if (tupleIndex + 4*tupleSize > BB.capacity()){
	    	fillFlush();
			BB.putInt(0, toAdd.data.size());
		}
		addTuple(toAdd);
	}
	
	/*
	 * Method that is used to truly clear a buffer by setting unused portion to be 0s
	 */
	public void fillZeros() {
		for (int i = tupleIndex; i < BB.capacity(); ++i)
			BB.put(i, (byte) 0);//fill with 0s
	}
	
	/*
	 * Method that is used to flush the buffer to file and clear the buffer
	 */
	public void flush() throws IOException {
		tupleIndex = 8;
		tupleCurrentCounts = 0;
		BB.clear();
		FC.write(BB);
		BB.clear();
	}
	
	/*
	 * Method that is used to fill unused portion of buffer with 0s and then flush the buffer to file
	 */
	public void fillFlush() throws IOException {
		fillZeros();
		flush();
	}
	
	/*
	 * Method that is used to close an open file
	 */
	public void close() throws IOException{
		FC.close();
	}
	
	/*
	 * Method that is used to reset tuple writer
	 */
	public void reset() throws IOException {
		FC.close();
	}

	
}
