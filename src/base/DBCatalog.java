package base;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

/*
 * DBCatalog
 * Catalog of this database, including input/output path and all the schema information of its tables loaded from schema.txt.
 * Singleton pattern because only one catalog instance is needed for every database.
 * 
 * @author Enze Zhou ez242
 */
public final class DBCatalog {
	
	private static DBCatalog catalog = new DBCatalog();		// The only instance of this class.
	
	public int pageSize = 4096;								// page size of files.
	public String inputPath = "";							// inputPath from cmd with a / at the end.
	public String outputPath = "";							// outputPath from cmd with a / at the end.
	public String tempPath = "";							// tempPath from cmd with a / at the end.
	public HashMap<String, Vector<String>> tables = null;	// Schema information of all the tables saved as a dictionary.
															// Key is the table name and value is a vector of all the column names of that table.
	public HashMap<String, String> indexKeys = null;		// Map names of relations who has an index to the key column name of these indexes.
	public HashMap<String, Integer> indexClustered = null;	// Map names of relations who has an index to whether the index is clustered.
	
	public boolean buildIndexes = false;					// Whether build index is required.
	public boolean evaluateQueries = false;					// Whether evaluate queries is required.
	
	/*
	 * Constructor is private for singleton pattern.
	 */
	private DBCatalog() {}
	
	/*
	 * Get the only instance of this class.
	 * @return the only instance of this class.
	 */
	public static DBCatalog getCatalog() { return catalog; }
	
	/*
	 * Use inputPath and outputPath to build catalog information.
	 * @param inputPath
	 * 		input path from cmd line.
	 * @param outputPath
	 * 		output path from cmd line.
	 */
	public void setSchema(String configPath) throws IOException {
		
		// Read configuration file.
		BufferedReader configReader = new BufferedReader(new FileReader(configPath));
		String configLine = null;
		configLine = configReader.readLine();
		this.inputPath = configLine.trim() + (configLine.contains("/")?"/":"\\");
		configLine = configReader.readLine();
		this.outputPath = configLine.trim() + (configLine.contains("/")?"/":"\\");
		configLine = configReader.readLine();
		this.tempPath = configLine.trim() + (configLine.contains("/")?"/":"\\");
		configLine = configReader.readLine();
		buildIndexes = (Integer.valueOf(configLine.trim()) == 1);
		configLine = configReader.readLine();
		evaluateQueries = (Integer.valueOf(configLine.trim()) == 1);
		configReader.close();
		
		tables = new HashMap<>();
		
		// Read from schema.txt to build schema information.
		String append = (this.inputPath.contains("/") ? "db/schema.txt" : "db\\schema.txt");
		BufferedReader schemaReader = new BufferedReader(new FileReader(this.inputPath + append));
		String schemaLine = null;
		while ((schemaLine = schemaReader.readLine()) != null) {
			String [] columns = schemaLine.trim().split(" ");
			Vector<String> columnsVec = new Vector<>();
			for (int i = 1; i < columns.length; ++i) {
				columnsVec.add(columns[i]);
			}
			tables.put(columns[0], columnsVec);
		}
		schemaReader.close();
		
		// Read from index info file to get index setting.
		indexKeys = new HashMap<>();
		indexClustered = new HashMap<>();
		append = (this.inputPath.contains("/") ? "db/index_info.txt" : "db\\index_info.txt");
		BufferedReader indexesReader = new BufferedReader(new FileReader(this.inputPath + append));
		String indexLine = null;
		while ((indexLine = indexesReader.readLine()) != null) {
			String [] indexConfig = indexLine.trim().split(" ");
			indexKeys.put(indexConfig[0], indexConfig[1]);
			indexClustered.put(indexConfig[0], Integer.valueOf(indexConfig[2]));
		}
		indexesReader.close();
	}
	
	/*
	 * Print catalog information.
	 */
	public void print() {
		System.out.println("inputPath:\t" + inputPath);
		System.out.println("outputPath:\t" + outputPath);
		System.out.println("tempPath:\t" + tempPath);
		System.out.println("buildIndexes:\t" + buildIndexes);
		System.out.println("evaluateQueries:\t" + evaluateQueries);
		
		System.out.println("Table schemas:");
		Set<String> tableNames = tables.keySet();
		for (String tableName : tableNames) {
			System.out.print("\t" + tableName + ":\t");
			Vector<String> columnNames = tables.get(tableName);
			for (int j = 0; j < columnNames.size(); ++j) {
				System.out.print(columnNames.get(j) + " ");
			}
			System.out.println();
		}
		
		System.out.println("Index schemas:");
		Set<String> indexFileNames = indexKeys.keySet();
		for (String indexFileName : indexFileNames) {
			System.out.println("\t" + indexFileName + ":\t" + indexKeys.get(indexFileName) + "\t" + indexClustered.get(indexFileName));
		}
	}
}
