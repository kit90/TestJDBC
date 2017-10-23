import java.sql.*;
import java.util.Scanner;

public class TestJDBC
{
    private static void printSQLExceptions(SQLException ex) 
    {                
        while (ex != null) 
        {           
            System.err.println("\n" + ex.getClass().getName() + ":\n");
            System.err.println("SQLState: " + ex.getSQLState());
            System.err.println("Error code: " + ex.getErrorCode());
            System.err.println("Message: " + ex.getMessage());

            Throwable t = ex.getCause();
            while (t != null) 
            {
              System.err.println("Caused by: " + t);
              t = t.getCause();
            }

            System.err.println();
            ex = ex.getNextException();           
        }        
    }
    
    private static void printSQLWarnings(SQLWarning warning) 
    {
        while (warning != null) 
        {
            System.err.println("\n" + warning.getClass().getName() + ":\n");
            System.err.println("SQLState: " + warning.getSQLState());
            System.err.println("Error code: " + warning.getErrorCode());
            System.err.println("Message: " + warning.getMessage());

            Throwable t = warning.getCause();
            while (t != null) 
            {
              System.err.println("Caused by: " + t);
              t = t.getCause();
            }

            System.err.println();
            warning = warning.getNextWarning();
        }        
    }
    
    private static void displayResults(ResultSet rs) throws SQLException 
    {
        System.out.println();
        
        ResultSetMetaData rsmd = rs.getMetaData();

        int columnsNumber = rsmd.getColumnCount();

        /* Loop to get all the column labels. */
        for (int i = 1; i <= columnsNumber; i++) 
        {
            System.out.printf("%20.20s|", rsmd.getColumnLabel(i));  
        }
        System.out.println();

        /* Print a separator bar for the column labels. */
        for (int i = 1; i <= columnsNumber; i++) 
        {
            for (int j = 1; j <= 20; j++) 
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
            for (int i = 1; i <= columnsNumber; i++) 
            {
                String value = rs.getString(i);               
                System.out.printf("%20.20s|", rs.wasNull() ? "<NULL>" : value);                                    
            }
            System.out.println();
            rowCount++;
        }
        System.out.println("\n" + rowCount + " row(s) returned.\n");        
    }
    
    public static void processStatements(Scanner sc, DatabaseMetaData dbm, Statement stmt)
    {
        System.out.println("Enter SQL commands.\n"
                + "Type 'tables' to list the tables.\n"
                + "Type 'columns <table>' to list the columns of <table>.\n"
                + "Type 'quit' to quit.\n");

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
                    displayResults(rs);                          
                }
                else if (statementString.startsWith("columns"))
                {
                    String[] words = statementString.split("\\s+");
                    
                    /* Get the columns in all tables in the database. */
                    rs = dbm.getColumns(null, null, words[1], null);
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
                        System.out.println("\n" + rowCount + " row(s) affected.\n");
                    }                    
                }
            } 
            catch (SQLException ex)
            {
                printSQLExceptions(ex);
            }
            catch (ArrayIndexOutOfBoundsException ex)
            {
                System.err.println("<table> was not given.");
            }
            catch (Exception ex) 
            {
                ex.printStackTrace();
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
    
    /**
     * Implement the application.
     * @param args The command-line arguments.
     */
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
}
