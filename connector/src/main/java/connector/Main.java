package connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:file:/data/demo";

    //  Database credentials
    static final String USER = "sa";
    static final String PASS = "";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            //STEP 2: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            //STEP 3: Execute a query
            System.out.println("Drop table in given database...");
            stmt = conn.createStatement();
            String sql =  "DROP TABLE REGISTRATION IF EXISTS;";
            stmt.executeUpdate(sql);

            sql = "CREATE TABLE REGISTRATION (id INTEGER not NULL, first VARCHAR(255),last VARCHAR(255), age INTEGER, PRIMARY KEY (id));";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");

            sql = "Insert into Registration(id, first, last) values (1, 'first', 'last');" +
                    "CREATE Hash INDEX prime_index ON REGISTRATION(id);" +
                    "CREATE Hash INDEX non_prime_index ON REGISTRATION(first);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(id, first, last) values (2, 'first', 'last');";
            stmt.executeUpdate(sql);

            sql = "CREATE INDEX non_prime_index_age ON REGISTRATION(age);";
            stmt.executeUpdate(sql);
            System.out.println("\nCreated table in given database...");

            sql = "Select * from Registration";
            stmt.executeQuery(sql);
            System.out.println("\nCreated table in given database...");

            sql = "Select age from Registration";
            stmt.executeQuery(sql);
            System.out.println("\nCreated table in given database...");

            sql = "Select id from Registration";
            stmt.executeQuery(sql);

            sql = "Select age, id from Registration";
            stmt.executeQuery(sql);
            System.out.println("\n");

            sql = "Select last from Registration";
            stmt.executeQuery(sql);
            System.out.println("\n COUNT METHODS ====================================================================\n");

            sql = "Select count(age) from Registration";
            stmt.executeQuery(sql);
            System.out.println("\n");

            sql = "Select count(last) from Registration";
            stmt.executeQuery(sql);
            System.out.println("\n");

            // STEP 4: Clean-up environment
            stmt.close();
            conn.close();
        } catch(SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            } // nothing we can do
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            } //end finally try
        } //end try
    }
}