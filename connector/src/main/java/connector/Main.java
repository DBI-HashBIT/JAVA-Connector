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

            stmt = conn.createStatement();
            sql =  "DROP TABLE REGISTRATION2 IF EXISTS;";
            stmt.executeUpdate(sql);

            /*
             This table contains
             insert operations before index creation and after index creation
             update and delete operations
             */

            sql = "CREATE TABLE REGISTRATION (id bigint auto_increment, first VARCHAR(255), middle VARCHAR(255), last VARCHAR(255), age INTEGER, checkcolumn INTEGER, PRIMARY KEY (id));";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");

            sql = "Insert into Registration(first, middle, last, age, checkcolumn) values ('first13', 'middle1', 'last14', 1, 1);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first, middle, last, age, checkcolumn) values ('first24', 'middle2', 'last25', 2, 2);";
            stmt.executeUpdate(sql);

            /*
             Create index for first column
             */

            sql = "CREATE hashbit Index secondary_index on REGISTRATION(first);";
            stmt.executeUpdate(sql);
            System.out.println("Created index in given database...");

            sql = "Insert into Registration(first, middle, last, age, checkcolumn) values ('first13', 'middle3', 'last36', 3, 3);";
            stmt.executeUpdate(sql);

            /*
             Create index for last column
             */

            sql = "CREATE hashbit Index secondary_index on REGISTRATION(last);";
            stmt.executeUpdate(sql);
            System.out.println("Created index in given database...");

            sql = "Insert into Registration(first, middle, last, age, checkcolumn) values ('first24', 'middle4', 'last14', 4, 4);";
            stmt.executeUpdate(sql);

            /*
             Create index for middle column
             */

            sql = "CREATE hashbit Index secondary_index on REGISTRATION(middle);";
            stmt.executeUpdate(sql);
            System.out.println("Created index in given database...");

            sql = "Insert into Registration(first, middle, last, age, checkcolumn) values ('first35', 'middle5', 'last25', 5, 5);";
            stmt.executeUpdate(sql);

//            /*
//            Check Error for non varchar column
//             */
//
//            try {
//                sql = "CREATE hashbit Index secondary_index on REGISTRATION(age);";
//                stmt.executeUpdate(sql);
//                System.out.println("Created index in given database...");
//                System.out.println("Error expected for age column");
//                System.exit(1);
//            } catch (Exception exception) {
//
//            }

            sql = "Insert into Registration(first, middle, last, age, checkcolumn) values ('first6', 'middle6', 'last36', 6, 6);";
            stmt.executeUpdate(sql);


            /*
             This section need to contain update indexes related test cases
             */
            System.out.println("============================================= Start Update Index ===================================================");
            System.out.println("============================================= End Update Index ===================================================");

            /*
             This section need to contain delete indexes related test cases
             */
            System.out.println("============================================= Start Delete Index ===================================================");
            System.out.println("============================================= End Delete Index ===================================================");

            /*
                Search rows with indexes
             */
            System.out.println("============================================= Start Search Index ===================================================");

            sql = "Select distinct checkcolumn from Registration where first = 'first13'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1, 3}, 1, 1);

            //AND Operations

            sql = "Select distinct checkcolumn from Registration where first = 'first13' and last = 'last14'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1}, 2, 1);

            sql = "Select * from Registration where first = 'first13' and last = 'last14' and middle = 'middle1'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1}, 3);

            sql = "Select * from Registration where first = 'first131'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{0}, 4, true);

            sql = "Select * from Registration where first = 'first13' and last = 'last25'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{0}, 5, true);

            sql = "Select * from Registration where first = 'first13' and last = 'last14' and middle = 'middle2'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{0}, 6, true);

            sql = "Select * from Registration where first = 'first13' and last = 'last141' and middle = 'middle1'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{0}, 7, true);

            // OR Operations

            sql = "Select distinct checkcolumn from Registration where first = 'first13' or last = 'last14'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1, 3, 4}, 8, 1);

            sql = "Select distinct checkcolumn from Registration where first = 'first13' or last = 'last14' or middle = 'middle1'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1, 3, 4}, 9, 1);

            sql = "Select * from Registration where first = 'first13' or last = 'last25'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1, 2, 3, 5}, 10);

            sql = "Select * from Registration where first = 'first13' or last = 'last14' or middle = 'middle2'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1, 2, 3 , 4}, 11);

            sql = "Select * from Registration where first = 'first13' or last = 'last141' or middle = 'middle1'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1, 3}, 12);

            System.out.println("============================================= End Search Index ===================================================");

            sql = "Select SUM(checkcolumn) from Registration where first = 'first13'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{4}, 13, 1);

            //AND Operations

            sql = "Select COUNT(checkcolumn) from Registration where first = 'first13' and last = 'last14'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1}, 14, 1);

            sql = "Select COUNT(*) from Registration where first = 'first13' and last = 'last14' and middle = 'middle1'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{1}, 15, 1);

            sql = "Select COUNT(*) from Registration where first = 'first131'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{0}, 16, 1);

            sql = "Select COUNT(*) from Registration where first = 'first13' and last = 'last25'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{0}, 17, 1);

            sql = "Select COUNT(*) from Registration where first = 'first13' and last = 'last14' and middle = 'middle2'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{0}, 18, 1);

            sql = "Select COUNT(*) from Registration where first = 'first13' and last = 'last141' and middle = 'middle1'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{0}, 19, 1);

            // OR Operations

            sql = "Select SUM(checkcolumn) from Registration where first = 'first13' or last = 'last14'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{8}, 20, 1);

            sql = "Select SUM(checkcolumn) from Registration where first = 'first13' or last = 'last14' or middle = 'middle1'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{8}, 21, 1);

            sql = "Select COUNT(*) from Registration where first = 'first13' or last = 'last25'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{4}, 22,1);

            sql = "Select COUNT(*) from Registration where first = 'first13' or last = 'last14' or middle = 'middle2'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{4}, 23,1);

            sql = "Select Count(*) from Registration where first = 'first24' or last = 'last141' or middle = 'middle1'";
            resultSet = stmt.executeQuery(sql);
            check(resultSet, new int[]{3}, 24,1);

            System.out.println("============================================= Start Aggregate Function Search Index ===================================================");



            System.out.println("============================================= End Aggregate Function Search Index ===================================================");

            /*
                Update rows with indexes
             */
            System.out.println("============================================= Start Update Row ===================================================");
            System.out.println("============================================= End Update Row ===================================================");

            /*
                Delete rows with indexes
             */
            System.out.println("============================================= Start Delete Row ===================================================");
            System.out.println("============================================= End Delete Row ===================================================");



//            System.out.println("-----------------------------------------------------UPDATE---------------------------------------------------------------------");
//
//            sql = "UPDATE Registration SET  first = 'update6', last = 'update6' WHERE ID = 6;";
//            stmt.executeUpdate(sql);
//
//            System.out.println("----------------------------------------------------DELETE----------------------------------------------------------------------");
////
//            sql = "DELETE FROM Registration WHERE id = 3;";
//            stmt.executeUpdate(sql);
//
//            sql = "Select distinct age from Registration where first = 'first159' and first = 'first159' and last = 'last159'";
//            resultSet = stmt.executeQuery(sql);
//            if (!resultSet.next()) {
//                System.out.println("NATURAL JOIN IS EMPTY");
//            }
//            while (resultSet.next()) {
//                System.out.println(resultSet.getInt((0)));
//            }

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

    public static void check(ResultSet resultSet, int[] expected, int index) throws SQLException {
        check(resultSet, expected, index, 5);
    }

    public static void check(ResultSet resultSet, int[] expected, int index, int checkIndex) throws SQLException {
        check(resultSet, expected, index, checkIndex, false);
    }

    public static void check(ResultSet resultSet, int[] expected, int index, Boolean isEmpty) throws SQLException {
        check(resultSet, expected, index, 5, isEmpty);
    }

    public static void check(ResultSet resultSet, int[] expected, int index, int checkIndex, Boolean isEmpty) throws SQLException {
        System.out.println("================================================================" + index + "===================================================================");
        if (expected == null) {
            return;
        }
        if (isEmpty) {
            expected = new int[]{0, 0, 0 , 0 , 0 , 0};
        }
        int i = 0;
        while (resultSet.next()) {
            try {
                if (!(((int) (resultSet.getObject(checkIndex))) == expected[i])) {
                    System.out.println("\n\n\n Matched in " + index + ", Expected:- " + expected[i] + ", Found:- " + resultSet.getObject(checkIndex).toString() + " !!!\n\n\n");
                }
            } catch (ClassCastException ex) {
                if (!(((long) (resultSet.getObject(checkIndex))) == expected[i])) {
                    System.out.println("\n\n\n Matched in " + index + ", Expected:- " + expected[i] + ", Found:- " + resultSet.getObject(checkIndex).toString() + " !!!\n\n\n");
                }
            }
            i++;
        }
        if (i ==0 && isEmpty) {
            return;
        }
        if (i ==0 && !isEmpty) {
            System.out.println("\n\n\nERROR in " + index + "!!!\n\n\n");
        }
    }
}
