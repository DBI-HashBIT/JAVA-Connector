import com.github.javafaker.Faker;
import org.h2.command.dml.Delete;

import java.sql.*;
import lombok.extern.slf4j.Slf4j;
import org.h2.table.Column;
import org.h2.value.TypeInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class Main {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:file:./data/demo";

    //  Database credentials
    static final String USER = "sa";
    static final String PASS = "";

    // faker for mock data
    private static final Faker faker = new Faker();

    private static final String TABLE_NAME = "demo";
    private static final Column PRIMARY_KEY = new Column("id", TypeInfo.TYPE_BIGINT);
    private static final List<Column> OTHER_COLUMNS = Arrays.asList(
            new Column("fname", TypeInfo.TYPE_VARCHAR),
            new Column("lname", TypeInfo.TYPE_VARCHAR)
    );

    private static final String FNAME_HASHBIT_INDEX_NAME = "FNAME_HASHBIT_INDEX";
    private static final String LNAME_HASHBIT_INDEX_NAME = "LNAME_HASHBIT_INDEX";

    private static final int FNAME_HASHBIT_INDEX_BUCKETS = 16;

    private static Connection conn = null;
    private static Statement stmt = null;

    public static void main(String[] args) {
        try {
            setUpDbConnection();
            initStmt();

            // drop able if exists
            executeSQL(SqlHelper.getDropTableSql(TABLE_NAME));

            // create table
            executeSQL(SqlHelper.getCreateTableSql(
                    TABLE_NAME,
                    PRIMARY_KEY,
                    OTHER_COLUMNS
            ));

            // create hashbit index
            executeSQL(SqlHelper.getCreateHashIndexSql(
                    FNAME_HASHBIT_INDEX_NAME,
                    TABLE_NAME,
                    OTHER_COLUMNS.get(0).getName(),
                    FNAME_HASHBIT_INDEX_BUCKETS
            ));

            // insert 1000 rows
            for (int i = 0; i < 1000; i++) {
                executeSQL(SqlHelper.getInsertSql(
                        TABLE_NAME,
                        OTHER_COLUMNS,
                        Arrays.asList(
                            faker.name().firstName(),
                            faker.name().lastName()
                        )
                ));
            }




//
//
//
//            sql = "CREATE TABLE REGISTRATION (id bigint auto_increment, first VARCHAR(255),last VARCHAR(255), age INTEGER, PRIMARY KEY (id));";
//            stmt.executeUpdate(sql);
//            System.out.println("Created table in given database...");
//
//            String sql = "CREATE HASHBIT Index BUCKETS 4 secondary_index_last on demo(fname);";
//            stmt.executeUpdate(sql);
//            System.out.println("Created index in given database...");
//
//            String sql = "Insert into demo(fname, lname) values ('first15', 'last15');";
//            stmt.executeUpdate(sql);
//
//            sql = "Insert into Registration(first, last, age) values ('first26', 'last26', 23);";
//            stmt.executeUpdate(sql);
//
//            sql = "Insert into Registration(first, last, age) values ('first26', 'last26', 23);";
//            stmt.executeUpdate(sql);
//
//            sql = "Insert into Registration(first, last, age) values ('brr26', 'brrlast26', 23);";
//            stmt.executeUpdate(sql);
//
//            sql = "UPDATE Registration SET  first = 'update6', last = 'update6' WHERE ID = 1;";
//            stmt.executeUpdate(sql);
//
//            sql = "Delete from Registration where id = '3'";
//            stmt.executeUpdate(sql);

            closeConnections();
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

    private static void executeSQL(String sql) throws SQLException {
        log.debug("Executing SQL: {}", sql);
        stmt.executeUpdate(sql);
    }

    private static void initStmt() throws SQLException {
        stmt = conn.createStatement();
    }

    private static void closeConnections() throws SQLException {
        // STEP 4: Clean-up environment
        stmt.close();
        conn.close();
    }

    private static void setUpDbConnection() throws ClassNotFoundException, SQLException {
        // STEP 1: Register JDBC driver
        Class.forName(JDBC_DRIVER);

        //STEP 2: Open a connection
        System.out.println("Connecting to database...");
        conn = DriverManager.getConnection(DB_URL,USER,PASS);
    }
}
