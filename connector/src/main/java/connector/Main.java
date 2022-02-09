package connector;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;

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

            sql = "Insert into Registration(id, first, last, age) values (1, 'first', 'last', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(id, first, last, age) values (2, 'first', 'last', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(id, first, last, age) values (3, 'first', 'last', 23);";
            stmt.executeUpdate(sql);

//            sql = "Insert into Registration(id, first) values (4, 'ff');";
//            stmt.executeUpdate(sql);

            sql = "Insert into Registration(id) values (5);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(id) values (6);";
            stmt.executeUpdate(sql);

            sql = "CREATE INDEX non_prime_index_age ON REGISTRATION(age);" +
                    "CREATE Hash INDEX prime_index ON REGISTRATION(id);" +
                    "CREATE Hash INDEX non_prime_index ON REGISTRATION(first);";
            stmt.executeUpdate(sql);
            System.out.println("\nCreated table in given database...");

//            sql = "Select * from Registration";
//            stmt.executeQuery(sql);
//            System.out.println("\nCreated table in given database...");
//
//            sql = "Select age from Registration";
//            stmt.executeQuery(sql);
//            System.out.println("\nCreated table in given database...");
//
//            sql = "Select id from Registration";
//            stmt.executeQuery(sql);
//
//            sql = "Select age, id from Registration";
//            stmt.executeQuery(sql);
//            System.out.println("\n");
//
//            sql = "Select last from Registration";
//            stmt.executeQuery(sql);
            System.out.println("\n COUNT METHODS ====================================================================\n");

//            sql = "Select count(first) from Registration";
//            ResultSet rs = stmt.executeQuery(sql);
//            System.out.println("Start of results");
//            while (rs.next()) {
//                System.out.println(rs.getObject(1).toString());
//            }
//            System.out.println("End of results");
//            System.out.println("\n");

//            sql = "Select count(last) from Registration";
//            stmt.executeQuery(sql);
//            System.out.println("\n");

            System.out.println("BOOLEAN OPERATORS==============================================================\n");

            sql = "Select * from Registration where first = 'first' and id = 1";
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println(resultSet.getInt(1));
                System.out.println(resultSet.getString(2));
            }

            System.out.println("\n\n");

            sql = "Select * from Registration where first = 'first' and id = 1 and first = 'first' or id = 2 and first = 'first'";
            ResultSet resultSet2 = stmt.executeQuery(sql);
            while (resultSet2.next()) {
                System.out.println(resultSet2.getInt(1));
                System.out.println(resultSet2.getString(2));
            }

            System.out.println("\n\n");

            sql = "Select Count(age) from Registration where first = 'first' and id = 1 and first = 'first' or id = 2 and first = 'first'";
            resultSet2 = stmt.executeQuery(sql);
            while (resultSet2.next()) {
                System.out.println(resultSet2.getInt(1));
            }

            System.out.println("\n\n");

            sql = "Select Count(*) from Registration where first = 'first' and id = 1 and first = 'first' or id = 2 and first = 'first' or id = 3";
            resultSet2 = stmt.executeQuery(sql);
            while (resultSet2.next()) {
                System.out.println(resultSet2.getInt(1));
            }

            System.out.println("\n\n");

            sql = "Select Count(age) from Registration where first = 'first' and id = 1 and first = 'first' or id = 2 and first = 'first' or id = 3";
            resultSet2 = stmt.executeQuery(sql);
            while (resultSet2.next()) {
                System.out.println(resultSet2.getInt(1));
            }

//            sql = "Select * from Registration where first = 'first' and age = 23";
//            stmt.executeQuery(sql);
//            System.out.println("\nCreated table in given database...\n");
//
//            sql = "Select first, age from Registration";
//            stmt.executeQuery(sql);
//            System.out.println("\n");

            System.out.println("=============Combine=========================================");

            sql = "Select Count(first), SUM(age) from Registration where first = 'first' and id = 1 or id = 2 and age = 23 or id = 3";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println(resultSet.getInt(1));
                System.out.println(resultSet.getInt(2));
            }

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