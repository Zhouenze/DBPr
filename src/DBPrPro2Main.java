import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

/*
 * DBPrPro2Main
 * Entrance of the whole project.
 */
public class DBPrPro2Main {

	/*
	 * main method of this project.
	 * @param args[0]
	 * 		input folder path without / at the end.
	 * @param args[1]
	 * 		output folder path without / at the end.
	 */
	public static void main(String[] args) {
		
		// Build DBCatalog first.
		try {
			DBCatalog.getCatalog().setSchema(args[0], args[1]);
		} 
		catch (IOException e) {
			System.err.println("IOException occurred when building catalog: " + e.toString());
		}
		
		// Output the catalog built.
		System.out.println("Built catalog:");
		DBCatalog.getCatalog().print();
		System.out.println();
		
		try {
			// Initialize a SqlParser.
			CCJSqlParser sqlParser = new CCJSqlParser(new FileReader(DBCatalog.getCatalog().inputPath + "queries.sql"));
			Statement statement;
			
			// For every select statement.
			int i = 1;
			while ((statement = sqlParser.Statement()) != null) {
				try {
					// Output the original statement.
					System.out.println("Read:\t" + statement);
					// Parse it and show parse result.
					myParser myPar = new myParser();
					Operator root = myPar.parseSelect((Select) statement);
					root.print();
					root.dump(null);
					System.out.println();
					root.reset();
					root.dump(new FileOutputStream(DBCatalog.getCatalog().outputPath + "query" + i++));

				// Catch every exception so that the program can go on to next statement.
				} catch (Exception e) {
					System.err.println("Exception occurred for statement: " + statement);
					System.err.println(e.toString());
					e.printStackTrace();
				}
			}
			
		} catch (Exception e) {
			System.err.println("Exception occurred with CCJSqlParser: " + e.toString());
		}
	}
}
