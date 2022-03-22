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

            sql = "CREATE TABLE REGISTRATION (id bigint auto_increment, first VARCHAR(255),last VARCHAR(255), age INTEGER, PRIMARY KEY (id));";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");

            sql = "CREATE TABLE REGISTRATION2 (id bigint auto_increment, PRIMARY KEY (id));";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");

            sql = "Insert into Registration(first, last, age) values ('first159', 'last159', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first, last, age) values ('first26', 'last26', 23);";
            stmt.executeUpdate(sql);


            sql = "Insert into Registration(first, last, age) values ('first37', 'last37', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first, last, age) values ('first48', 'last48', 23);";
            stmt.executeUpdate(sql);


            sql = "Insert into Registration(first, last, age) values ('first159', 'last159', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first, last, age) values ('first26', 'last26', 23);";
            stmt.executeUpdate(sql);


            sql = "CREATE hashbit Index secondary_index on REGISTRATION(first);";
            stmt.executeUpdate(sql);
            System.out.println("Created index in given database...");

            sql = "CREATE hashbit Index secondary_index_last on REGISTRATION(last);";
            stmt.executeUpdate(sql);
            System.out.println("Created index in given database...");

            System.out.println("?????????????????????????????  This is Part 02 ????????????????????????????????????????????????????????");

            sql = "Insert into Registration(first, last, age) values ('first37', 'last37', 23);";
            stmt.executeUpdate(sql);


            sql = "Insert into Registration(first, last, age) values ('first48', 'last48', 23);";
            stmt.executeUpdate(sql);


            sql = "Insert into Registration(first, last, age) values ('first159', 'last159', 23);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration() values ();";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration() values ();";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(age) values (31);";
            stmt.executeUpdate(sql);

            sql = "Insert into Registration(first) values ('first13');";
            stmt.executeUpdate(sql);

//            null => 10, 11, 12

            System.out.println("-----------------------------------------------------UPDATE---------------------------------------------------------------------");

//            sql = "UPDATE Registration SET  first = 'update6', last = 'update6' WHERE ID = 6;";
//            stmt.executeUpdate(sql);
//
//            sql = "UPDATE Registration SET  first = 'update8', last = 'update8' WHERE ID = 8;";
//            stmt.executeUpdate(sql);
//
//            sql = "UPDATE Registration SET  first = 'update1', last = 'update1' WHERE ID = 1;";
//            stmt.executeUpdate(sql);
//
//            sql = "UPDATE Registration SET  first = 'update3', last = 'update3' WHERE ID = 3;";
//            stmt.executeUpdate(sql);
//
//            sql = "UPDATE Registration SET  first = 'first13', last = 'last13' WHERE ID = 10;";
//            stmt.executeUpdate(sql);
//
//            sql = "UPDATE Registration SET  first = NULL, last = NULL WHERE ID = 11;";
//            stmt.executeUpdate(sql);

            System.out.println("----------------------------------------------------DELETE----------------------------------------------------------------------");
//
//
//            sql = "DELETE FROM Registration WHERE id = 3;";
//            stmt.executeUpdate(sql);
//
//            sql = "DELETE FROM Registration WHERE id = 10;";
//            stmt.executeUpdate(sql);
//
//            sql = "DELETE FROM Registration WHERE id = 1;";
//            stmt.executeUpdate(sql);
//
            System.out.println("-----------------------------------------------------------SELECT-------------------------------------------------------");

            sql = "Select first,last from Registration where first = 'first159'";
            resultSet = stmt.executeQuery(sql);

            System.out.println("1.===========================================================================================================");

//            sql = "Select 1";
//            resultSet = stmt.executeQuery(sql);
//
//            System.out.println("2.===========================================================================================================");
//
//            sql = "Select Count(first) from Registration";
//            resultSet = stmt.executeQuery(sql);
//
//            System.out.println("3.===========================================================================================================");
//
//            sql = "Select Sum(age) from Registration";
//            resultSet = stmt.executeQuery(sql);

            System.out.println("4.===========================================================================================================");

            sql = "Select Distinct age from Registration where first = 'first159'";
            resultSet = stmt.executeQuery(sql);

            System.out.println("5.===========================================================================================================");

//            sql = "Select Distinct SUM(age) from Registration";
//            resultSet = stmt.executeQuery(sql);

            System.out.println("6.===========================================================================================================");

            sql = "Select Distinct SUM(age) from Registration where last = 'last159'";
            resultSet = stmt.executeQuery(sql);

            System.out.println("7.===========================================================================================================");

//            sql = "Select first, avg(age) from Registration group by(first)";
//            resultSet = stmt.executeQuery(sql);
//            while (resultSet.next()) {
//                System.out.println("==============Result Row============================");
//                System.out.println(resultSet.getString(1));
//                System.out.println(resultSet.getInt(2));
//                System.out.println("==============Result Row============================");
//            }
//
            System.out.println("8.===========================================================================================================");

            sql = "Select distinct age from Registration where first = 'first159'";
            resultSet = stmt.executeQuery(sql);

            System.out.println("9.=================================================Natural Join==========================================================");

            sql = "Select distinct age from Registration where first = 'first159' or last = 'last2'";
            resultSet = stmt.executeQuery(sql);
            if (!resultSet.next()) {
                System.out.println("NATURAL JOIN IS EMPTY");
            }
            while (resultSet.next()) {
                System.out.println(resultSet.getInt((0)));
            }

//            System.out.println("10.================================================= Order By ==========================================================");
//
////            sql = "Select age from Registration where first = 'first' order by age";
////            resultSet = stmt.executeQuery(sql);
////
////            System.out.println("11.================================================= Order By ==========================================================");
////
////            sql = "Select first, avg(age) from Registration where first = 'first2' group by(first) having avg(age) > 0";
////            resultSet = stmt.executeQuery(sql);
////
////            while (resultSet.next()) {
////                System.out.println("==============Result Row============================");
////                System.out.println(resultSet.getString(1));
////                System.out.println(resultSet.getInt(2));
////                System.out.println("==============Result Row============================");
////            }
//
//            System.out.println("12.===========================================================================================================");
//
////            sql = "DELETE FROM Registration WHERE id = 1;";
////            stmt.executeUpdate(sql);
////
////
////            sql = "DELETE FROM Registration WHERE id = 3;";
////            stmt.executeUpdate(sql);
////
////            sql = "DELETE FROM Registration WHERE id = 2;";
////            stmt.executeUpdate(sql);
//
////            while (resultSet.next()) {
////                System.out.println("==============Result Row============================");
////                System.out.println(resultSet.getString(1));
////                System.out.println(resultSet.getString(2));
////                System.out.println("==============Result Row============================");
////            }
//
//            System.out.println("=====================================13=======================================================");
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
}
