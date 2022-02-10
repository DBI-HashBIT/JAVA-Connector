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
            String sql; ResultSet resultSet;
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            //STEP 2: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            //STEP 3: Execute a query
            System.out.println("Drop table in given database...");
            stmt = conn.createStatement();
            sql =  "DROP TABLE REGISTRATION IF EXISTS;";
            stmt.executeUpdate(sql);

            sql = "CREATE TABLE REGISTRATION (id bigint auto_increment, first VARCHAR(255),last VARCHAR(255), age INTEGER, PRIMARY KEY (id));";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");

            sql = "Insert into Registration(first, last, age) values ('first', 'last', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first, last, age) values ('first', 'last', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first, last, age) values ('first', 'last', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first, last, age) values ('first2', 'last', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration() values ();";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration() values ();";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(age) values (31);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first) values ('first');";
            stmt.executeUpdate(sql);

            System.out.println("\nCreated table in given database...\n");

            System.out.println("===========================================================================================================");
            System.out.println("1. Start column equal to value");
            sql = "Select * from Registration where first = 'first'";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("ID:- " + resultSet.getInt(1));
                System.out.println("First:- " + resultSet.getString(2));
                System.out.println("last:- " + resultSet.getString(3));
                System.out.println("Age:- " + resultSet.getInt(4));
                System.out.println("============ Result Row =================");
            }
            System.out.println("1. End column equal to value");

            System.out.println("===========================================================================================================");

            System.out.println("2. Select specific column with Start column equal to value");
            sql = "Select last,age from Registration where first = 'first'";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("last:- " + resultSet.getString(1));
                System.out.println("Age:- " + resultSet.getInt(2));
                System.out.println("============ Result Row =================");
            }
            System.out.println("2. End Select specific column with Start column equal to value");

            System.out.println("===========================================================================================================");

            System.out.println("3. Start many columns AND equal to value");
            sql = "Select * from Registration where first = 'first' and age = 23 and last = 'last'";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("ID:- " + resultSet.getInt(1));
                System.out.println("First:- " + resultSet.getString(2));
                System.out.println("last:- " + resultSet.getString(3));
                System.out.println("Age:- " + resultSet.getInt(4));
                System.out.println("============ Result Row =================");
            }
            System.out.println("3. End many columns AND equal to value");

            System.out.println("===========================================================================================================");

            System.out.println("4. Start many columns or equal to value");
            sql = "Select * from Registration where first = 'first' or age = 23 or last = 'last' or id = 5";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("ID:- " + resultSet.getInt(1));
                System.out.println("First:- " + resultSet.getString(2));
                System.out.println("last:- " + resultSet.getString(3));
                System.out.println("Age:- " + resultSet.getInt(4));
                System.out.println("============ Result Row =================");
            }
            System.out.println("4. End many columns or equal to value");

            System.out.println("===========================================================================================================");

            System.out.println("5. Start many columns and/or equal to value");
            sql = "Select age,last from Registration where first = 'first' and age = 23 or last = 'last' and id = 5";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("last:- " + resultSet.getString(2));
                System.out.println("Age:- " + resultSet.getInt(1));
                System.out.println("============ Result Row =================");
            }
            System.out.println("5. End many columns and/or equal to value");

            System.out.println("===========================================================================================================");

            System.out.println("6. Select specific column count with Start column equal to value");
            sql = "Select Count(last),SUM(age),Count(first) from Registration where first = 'first'";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("Count(last):- " + resultSet.getInt(1));
                System.out.println("SUM(age):- " + resultSet.getInt(2));
                System.out.println("COunt(first):- " + resultSet.getInt(3));
                System.out.println("============ Result Row =================");
            }
            System.out.println("6. End Select specific column count with Start column equal to value");

            System.out.println("===========================================================================================================");

            System.out.println("7. Select specific columns count with Start column equal to value");
            sql = "Select Count(last),SUM(age),Count(first) from Registration where first = 'first' and age = 23 or age = 23 and first = 'first'";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("Count(last):- " + resultSet.getInt(1));
                System.out.println("SUM(age):- " + resultSet.getInt(2));
                System.out.println("COunt(first):- " + resultSet.getInt(3));
                System.out.println("============ Result Row =================");
            }
            System.out.println("7. End Select specific columns count with Start column equal to value");

            System.out.println("===========================================================================================================");

            System.out.println("8. Select specific columns count with Start column equal to value");
            sql = "Select Count(last),SUM(age),Count(first) from Registration where age = 31";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("Count(last):- " + resultSet.getInt(1));
                System.out.println("SUM(age):- " + resultSet.getInt(2));
                System.out.println("COunt(first):- " + resultSet.getInt(3));
                System.out.println("============ Result Row =================");
            }
            System.out.println("8. End Select specific columns count with Start column equal to value");

            System.out.println("=======================Emty Queries Started====================================================================================");
            
            System.out.println("===========================================================================================================");

            System.out.println("9. Start Empty many columns and/or equal to value");
            sql = "Select age,last from Registration where first = 'first' and age = 23 and last = 'last' and id = 5";
            resultSet = stmt.executeQuery(sql);
            if (resultSet.next() == false) {
                System.out.println("EMPTY RESLUT SET AND ITS EXPECTED");
            }
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("last:- " + resultSet.getString(2));
                System.out.println("Age:- " + resultSet.getInt(1));
                System.out.println("============ Result Row =================");
            }
            System.out.println("9. End Empty many columns and/or equal to value");

            System.out.println("===========================================================================================================");

            System.out.println("10. Start Empty Count and SUm");
            sql = "Select Count(last),SUM(age),Count(first) from Registration where first = 'firstt' and age = 23 and age = 23 and first = 'first'";
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println("============ Result Row =================");
                System.out.println("Count(last):- " + resultSet.getInt(1));
                System.out.println("SUM(age):- " + resultSet.getInt(2));
                System.out.println("COunt(first):- " + resultSet.getInt(3));
                System.out.println("============ Result Row =================");
            }
            System.out.println("10. End Empty Count and Sun");

            System.out.println("===========================================================================================================");

            System.out.println("=======================Emty Queries Ended======================================================================================");
            

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