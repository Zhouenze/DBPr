package base;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/*
 * This class is used to generate test data and use TupleWriter to write it in binary format to file.
 * 
 * @author Enze Zhou ez242
 */
public final class dataGenerator {
	
	/*
	 * Function that generate random test data.
	 * 
	 * @param
	 * 		path: the file path that test data should be put.
	 * 		seed: the seed for random number generator.
	 */
	public void generateData(String path, int seed) {
		
		try {
			Random rand = new Random();
			rand.setSeed(seed);

			// Output binary and human-readable file at the same time.
			TupleWriter TW = new TupleWriter(path);
			DataOutputStream dataOut = new DataOutputStream(new FileOutputStream(path + "readable"));
			
			// Each test data file has 6000 tuples and each of them has 6 columns.
			for (int i = 0; i < 6000; ++i) {
				Tuple tp = new Tuple();
				for (int j = 0; j < 6; ++j) {
					Integer temp = rand.nextInt(1000);
					tp.data.add(temp);
					dataOut.writeBytes((j == 0 ? temp.toString() : "," + temp.toString()));
				}
				TW.setNextTuple(tp);
				dataOut.write('\n');
			}
			
			TW.close();
			dataOut.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
