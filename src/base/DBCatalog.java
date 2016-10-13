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
public class DBCatalog {
	
	private static DBCatalog catalog = new DBCatalog();		// The only instance of this class.
	
	public String inputPath = "";							// inputPath from cmd with a / at the end.
	String outputPath = "";									// outputPath from cmd with a / at the end.
	public HashMap<String, Vector<String>> tables = null;	// Schema information of all the tables saved as a dictionary.
															// Key is the table name and value is a vector of all the column names of that table.
	
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
	public void setSchema(String inputPath, String outputPath) throws IOException {
		
		// Paths are appended with a / to simplify future usage.
		this.inputPath = inputPath + "/";
		this.outputPath = outputPath + "/";
		tables = new HashMap<>();
		
		// Read from schema.txt to build schema information.
		BufferedReader schemaReader = new BufferedReader(new FileReader(this.inputPath + "db/schema.txt"));
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
	}
	
	/*
	 * Print catalog information.
	 */
	public void print() {
		System.out.println("inputPath:\t" + inputPath);
		System.out.println("outputPath:\t" + outputPath);
		Set<String> tableNames = tables.keySet();
		for (String tableName : tableNames) {
			System.out.print(tableName + ":\t");
			Vector<String> columnNames = tables.get(tableName);
			for (int j = 0; j < columnNames.size(); ++j) {
				System.out.print(columnNames.get(j) + " ");
			}
			System.out.print('\n');
		}
	}
}
