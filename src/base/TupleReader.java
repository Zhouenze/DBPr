package base;

/*
 * A level of abstration to help with reading records stored in files
 * 
 * @author Weicheng Yu wy248
 */
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TupleReader {
	public String fileName;
	private FileChannel FC;
	private ByteBuffer BB;
	private int tupleIndex;
	private int tupleSize;
	private int tupleCounts;
	private int tupleCurrentCounts;
	
	@SuppressWarnings("resource")
	public TupleReader(String filename) throws IOException {
		fileName = filename;
		FC = new FileInputStream(filename).getChannel();
		BB = ByteBuffer.allocate(4096); //hardcoded page size
		readTupleInfo();
	}
	
	/*
	 * Method that is used to read meta data
	 */
	public int readTupleInfo() throws IOException {
		//read tuple meta data
		int ret = FC.read(BB);
		if (ret > 0){
			tupleSize = BB.getInt(0);
			tupleCounts = BB.getInt(4);
			tupleIndex = 8;
			tupleCurrentCounts = 0;	
		}
		return ret;
	}
	
	
	/*
	 * Method that is used to grab a record
	 */
	public Tuple getNextTuple() throws IOException{

		//if a page has no tuple, return null directly 
		//previously calling getNextTuple() will return a tuple object with no data set. Now it returns null
		if (tupleCounts == 0) return null;


		Tuple ret = new Tuple();
		if (tupleCurrentCounts < tupleCounts) {
			for (int i = 0; i < tupleSize; ++i) {
				ret.data.add(BB.getInt(tupleIndex));
				tupleIndex += 4;
			}
		}
		else{
			BB.clear();
			if (readTupleInfo() <= 0){
				return null;
			}
			for (int i = 0; i < tupleSize; ++i) {
				ret.data.add(BB.getInt(tupleIndex));
				tupleIndex += 4;
			}		
		}
		++tupleCurrentCounts;
		return ret;

	}
	
	/*
	 * Method that is used to close an open file
	 */
	public void close() throws IOException{
		FC.close();
	}
	
	
	/*
	 * Method that is used to reset tuple reader
	 */
	@SuppressWarnings("resource")
	public void reset() throws IOException {
		FC.close();
		FC = new FileInputStream(fileName).getChannel();
		BB.clear();
		readTupleInfo();
		
	}
	
	
	
	
	
	
	
	
}
