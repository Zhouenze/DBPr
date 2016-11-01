package base;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import logicalPlan.LogPlan;
import logicalPlan.LogPlanPrintVisitor;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import physicalPlan.PhyJoinSMJOp;
import physicalPlan.PhyPlan;
import physicalPlan.PhyPlanPrintVisitor;

/*
 * DBPrPro2Main
 * Entrance of the whole project.
 * 
 * @author Enze Zhou ez242
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
			DBCatalog.getCatalog().setSchema(args[0], args[1], args[2]);
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
					// Build logical plan.
					LogPlan plan = new LogPlan((Select) statement);
					//LogPlanPrintVisitor logPlanPrinter = new LogPlanPrintVisitor();
					//System.out.println(logPlanPrinter.printLogPlan(plan));
					//System.out.println("tables "+DBCatalog.getCatalog().tables.toString());
					// Build physical plan and run it.
					PhyPlan phyPlan = new PhyPlan(plan);
					PhyPlanPrintVisitor phyPlanPrinter = new PhyPlanPrintVisitor();
					System.out.println("php plan printer" + phyPlanPrinter.printPhyPlan(phyPlan));
//					
					//phyPlan.root.dumpReadable(null);
					System.out.println();
					phyPlan.root.reset();
//					phyPlan.root.dump(new FileOutputStream(DBCatalog.getCatalog().outputPath + "query" + i++));
//					phyPlan.root.dumpReadable(new FileOutputStream(DBCatalog.getCatalog().outputPath + "query" + i++));
//					((PhyJoinSMJOp)phyPlan.root.child.child).rChild.dumpReadable(new FileOutputStream(DBCatalog.getCatalog().outputPath + "inner"));
//					((PhyJoinSMJOp)phyPlan.root.child.child).child.dumpReadable(new FileOutputStream(DBCatalog.getCatalog().outputPath + "outer"));
					System.out.println(((PhyJoinSMJOp)phyPlan.root.child.child).rChild.schema);
//					Tuple tp = ((PhyJoinSMJOp)phyPlan.root.child.child).rChild.getNextTuple();
//					tp.print();

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
