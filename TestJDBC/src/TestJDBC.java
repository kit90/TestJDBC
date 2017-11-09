import java.sql.*;
import java.util.Scanner;

public class TestJDBC
{
    private static final int DISPLAY_MAX = 50; // Arbitrary limit on column width to display.
    private static final int DISPLAY_FORMAT_EXTRA = 3; // Per column extra display bytes ( <data> |).
    private static final int NULL_SIZE = 6; // <NULL>.
    
    public static void main(String[] args)
    {
        String databaseURL = null;
        String username = null;
        String password = null;
        
        try
        {
            databaseURL = args[0];
            username = args[1];
            password = args[2];
        } 
        catch (ArrayIndexOutOfBoundsException e)
        {
            System.err.println("Enter arguments: <database URL> <username> <password>");
            System.exit(1);
        }
        
        Connection conn = null;
        Statement stmt = null;
        
        Scanner sc = new Scanner(System.in);

        try 
        {
            /* Create the connection object. */  
            conn = DriverManager.getConnection(databaseURL, username, password); 
            printSQLWarnings(conn.getWarnings());
            
            DatabaseMetaData dbm = conn.getMetaData();
            
            /* Create the statement object. */
            stmt = conn.createStatement();
            
            processStatements(sc, dbm, stmt);
        } 
        catch (SQLException ex) 
        {
            printSQLExceptions(ex);       
        }  
        finally 
        {      
            /* Perform clean up. */            
            sc.close();                        
            
            try 
            {
                if (stmt != null) 
                {
                    stmt.close();
                }
            } 
            catch (SQLException ex) 
            {
                printSQLExceptions(ex);                                       
            }

            try 
            {
                if (conn != null) 
                {
                    conn.close();
                }
            } 
            catch (SQLException ex) 
            {
                printSQLExceptions(ex);                                       
            }
        }   
    }
    
    private static void processStatements(Scanner sc, DatabaseMetaData dbm, Statement stmt)
    {
        System.out.printf("Enter SQL commands.%n"
                + "Type 'tables' to list the tables.%n"
                + "Type 'columns <table>' to list the columns of <table>.%n"
                + "Type 'quit' to quit.%n%n");

        ResultSet rs = null;

        System.out.print("SQL> ");
        while (sc.hasNext()) 
        {                
            try
            {
                String statementString = sc.nextLine();       
                
                if (statementString.startsWith("tables"))
                {
                    /* Get the tables in the database. */
                    rs = dbm.getTables(null, null, null, null);
                    printSQLWarnings(dbm.getConnection().getWarnings());
                    
                    displayResults(rs);                          
                }
                else if (statementString.startsWith("columns"))
                {
                    String[] words = statementString.split("\\s+");
                    
                    /* Get the columns in all tables in the database. */
                    rs = dbm.getColumns(null, null, words[1], null);
                    printSQLWarnings(dbm.getConnection().getWarnings());
                    
                    displayResults(rs);                    
                }
                else if (statementString.startsWith("quit"))
                {
                    break;
                }
                else
                {   
                    /* Execute the query. */
                    boolean hasResultSet = stmt.execute(statementString);
                    printSQLWarnings(stmt.getWarnings());
                    
                    if (hasResultSet) 
                    {
                        rs = stmt.getResultSet();
                        displayResults(rs);                    
                    } 
                    else 
                    {
                        int rowCount = stmt.getUpdateCount();
                        System.out.printf("%n" + rowCount + " row(s) affected.%n%n");
                    }                    
                }
            } 
            catch (SQLException ex)
            {
                printSQLExceptions(ex);
            }
            catch (Exception ex) 
            {
                ex.printStackTrace();
                System.err.println();
            }
            finally
            {
                /* Perform clean up. */            
                try 
                {
                    if (rs != null) 
                    {
                        rs.close();
                    }
                } 
                catch (SQLException ex) 
                {
                    printSQLExceptions(ex);                       
                }   
            }
            
            System.out.print("SQL> ");
        }                      
    }
    
    private static void displayResults(ResultSet rs) throws SQLException 
    {
        System.out.println();
        
        ResultSetMetaData rsmd = rs.getMetaData();

        int columnCount = rsmd.getColumnCount();

        int[] displaySize = new int[columnCount];
        boolean[] isChar = new boolean[columnCount];
        
        /* Loop to get all the column labels. */
        for (int i = 0; i < columnCount; i++) 
        {
            int columnDisplaySize = rsmd.getColumnDisplaySize(i + 1);
            String columnLabel = rsmd.getColumnLabel(i + 1);
            
            displaySize[i] = Math.max(columnDisplaySize, NULL_SIZE);
            displaySize[i] = Math.max(displaySize[i], columnLabel.length());
            displaySize[i] = Math.min(displaySize[i], DISPLAY_MAX);
            
            int columnType = rsmd.getColumnType(i + 1);
            isChar[i] = columnType == Types.CHAR
                    || columnType == Types.VARCHAR
                    || columnType == Types.LONGVARCHAR;
            
            System.out.printf(" %-" + displaySize[i] + "." + displaySize[i] + "s |", columnLabel);  
        }
        System.out.println();

        /* Print a separator bar for the column labels. */
        for (int i = 0; i < columnCount; i++) 
        {
            for (int j = 0; j < displaySize[i] + DISPLAY_FORMAT_EXTRA - 1; j++) 
            {
                System.out.print("-");
            }
            System.out.print("|");
        }
        System.out.println();

        /* Loop through the ResultSet to fetch data. */
        int rowCount = 0;
        while (rs.next()) 
        {
            for (int i = 0; i < columnCount; i++) 
            {
                String value = rs.getString(i + 1);
                
                if (rs.wasNull())
                {
                    System.out.printf(" %" + (isChar[i] ? "-" : "") + displaySize[i] + "." 
                            + displaySize[i] + "s |", "<NULL>");
                }
                else
                {
                    System.out.printf(" %" + (isChar[i] ? "-" : "") + displaySize[i] + "." 
                            + displaySize[i] + "s |", value);
                }
            }
            System.out.println();
            rowCount++;
        }
        System.out.printf("%n" + rowCount + " row(s) returned.%n%n");        
    }
    
    private static void printSQLExceptions(SQLException ex) 
    {           
        while (ex != null) 
        {           
            System.err.print("[SQLState: " + ex.getSQLState() + "]");
            System.err.println("[Error code: " + ex.getErrorCode() + "]");
            ex.printStackTrace();
            System.err.println();
            
            ex = ex.getNextException();           
        } 
    }
    
    private static void printSQLWarnings(SQLWarning warning) 
    {
        while (warning != null) 
        {
            System.err.print("[SQLState: " + warning.getSQLState() + "]");
            System.err.println("[Error code: " + warning.getErrorCode() + "]");
            warning.printStackTrace();
            System.err.println();
            
            warning = warning.getNextWarning();
        }        
    }
}
