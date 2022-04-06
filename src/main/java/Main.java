import com.github.javafaker.Faker;

import java.sql.*;
import lombok.extern.slf4j.Slf4j;
import org.h2.table.Column;
import org.h2.util.Profiler;
import org.h2.value.TypeInfo;
import org.apache.commons.lang3.time.StopWatch;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class Main {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:file:./data/demo";

    //  Database credentials
    static final String USER = "sa";
    static final String PASS = "";

    private static final String TABLE_NAME = "demo";
    private static final Column PRIMARY_KEY = new Column("id", TypeInfo.TYPE_BIGINT);
    private static final List<Column> OTHER_COLUMNS = Arrays.asList(
            new Column("fname", TypeInfo.TYPE_VARCHAR),
            new Column("indexedfname", TypeInfo.TYPE_VARCHAR),
            new Column("lname", TypeInfo.TYPE_VARCHAR),
            new Column("indexedlname", TypeInfo.TYPE_VARCHAR)
    );

    private static final String FNAME_HASHBIT_INDEX_NAME = "FNAME_HASHBIT_INDEX";
    private static final String LNAME_HASHBIT_INDEX_NAME = "LNAME_HASHBIT_INDEX";

    private static final int FNAME_HASHBIT_INDEX_BUCKETS = 1024;

    private static final int DATA_ROWS = 60000;
    private static final int DUPLICATION_RATE = 1;

    private static Connection conn = null;
    private static Statement stmt = null;
    private static Statement searchStmt1 = null;
    private static Statement searchStmt2 = null;

    // mock data
    private static final List<String> fNames = TestHelper.getNMockFirstNames(DATA_ROWS/DUPLICATION_RATE);
    private static final List<String> lNames = TestHelper.getNMockLastNames(DATA_ROWS/DUPLICATION_RATE);

    public static void main(String[] args) {
        try {
            setUpDbConnection();
            initStmt();

            // drop able if exists
            executeUpdateSQL(SqlHelper.getDropTableSql(TABLE_NAME));

            // create table
            executeUpdateSQL(SqlHelper.getCreateTableSql(
                    TABLE_NAME,
                    PRIMARY_KEY,
                    OTHER_COLUMNS
            ));

            // create fname hashbit index
            executeUpdateSQL(SqlHelper.getCreateHashIndexSql(
                    FNAME_HASHBIT_INDEX_NAME,
                    TABLE_NAME,
                    OTHER_COLUMNS.get(1).getName(),
                    FNAME_HASHBIT_INDEX_BUCKETS
            ));

            // create lname hashbit index
            executeUpdateSQL(SqlHelper.getCreateHashIndexSql(
                    LNAME_HASHBIT_INDEX_NAME,
                    TABLE_NAME,
                    OTHER_COLUMNS.get(3).getName(),
                    FNAME_HASHBIT_INDEX_BUCKETS
            ));


            // insert mock data
            Random fnameRand = new Random(0);
            Random lnameRand = new Random(1);

            log.info("Inserting mock data...");

            for (int i = 0; i < DATA_ROWS; i++) {
                if (i % 1000 == 0) {
                    //log.info("Inserting row {}", i);
                }
                String fname = fNames.get(fnameRand.nextInt(fNames.size()));
                String lname = lNames.get(lnameRand.nextInt(lNames.size()));
                executeUpdateSQL(SqlHelper.getInsertSql(
                        TABLE_NAME,
                        OTHER_COLUMNS,
                        Arrays.asList(
                            fname,
                            fname,
                            lname,
                            lname
                        )
                ));
            }

            // query data
//            timeSelect();
//            testAndSelect();
            timeOrSelect();

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

    private static void testSelect() throws SQLException {
        for (int j = 0; j < DATA_ROWS / 4; j++) {
            int rand = ThreadLocalRandom.current().nextInt(0, fNames.size());
            String fname = fNames.get(rand);
            ResultSet indexedRs = executeSelectUsingColumn(0, fname, searchStmt1);
            ResultSet nonIndexedRs = executeSelectUsingColumn(1, fname, searchStmt2);

            checkEquality(indexedRs, nonIndexedRs);
        }
    }


    private static void timeOrSelect() throws SQLException, InterruptedException {

        int testFraction = DATA_ROWS / 4;

        Random fnameRand = new Random(2);
        Random lnameRand = new Random(3);

        /*TimeUnit.SECONDS.sleep(10);

        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        for (int j = 0; j < testFraction; j++) {
            String fname = fNames.get(fnameRand.nextInt(fNames.size()));
            String lname = lNames.get(lnameRand.nextInt(lNames.size()));
            ResultSet indexedRs = executeOrSelectUsingColumns(
                    Arrays.asList(1,3),
                    Arrays.asList(fname, lname),
                    searchStmt1
            );
        }
        stopwatch.stop();
        log.info("Indexed OR select took {}", stopwatch.formatTime());*/

        TimeUnit.SECONDS.sleep(10);

        StopWatch stopwatch2 = new StopWatch();
        stopwatch2.start();
        for (int j = 0; j < testFraction; j++) {
            String fname = fNames.get(fnameRand.nextInt(fNames.size()));
            String lname = lNames.get(lnameRand.nextInt(lNames.size()));
            ResultSet nonIndexedRs = executeOrSelectUsingColumns(
                    Arrays.asList(0,2),
                    Arrays.asList(fname, lname),
                    searchStmt2
            );
        }
        stopwatch2.stop();
        log.info("Non-indexed OR select took {}", stopwatch2.formatTime());


    }

    private static void timeSelect() throws SQLException, InterruptedException {
        int testFraction = DATA_ROWS / 4;

        List<Integer> randInts = new Random(0).ints(0, fNames.size()).distinct().limit(testFraction).boxed().collect(Collectors.toList());

//        TimeUnit.SECONDS.sleep(10);
//
//        StopWatch stopwatch = new StopWatch();
//        stopwatch.start();
//        for (int j = 0; j < testFraction; j++) {
//            String fname = fNames.get(randInts.get(j));
//            ResultSet indexedRs = executeSelectUsingColumn(0, fname, searchStmt1);
//        }
//        stopwatch.stop();
//        log.info("Indexed select took {}", stopwatch.formatTime());

        TimeUnit.SECONDS.sleep(10);

        StopWatch stopwatch2 = new StopWatch();
        stopwatch2.start();
        for (int j = 0; j < testFraction; j++) {
            String fname = fNames.get(randInts.get(j));
            ResultSet nonIndexedRs = executeSelectUsingColumn(1, fname, searchStmt2);
        }
        stopwatch2.stop();
        log.info("Non-indexed select took {}", stopwatch2.formatTime());


    }


    private static ResultSet executeSelectUsingColumn(int column, String fname, Statement searchStmt) throws SQLException {
        return executeQuerySQL(SqlHelper.getSelectSql(
                TABLE_NAME,
                Collections.singletonList(PRIMARY_KEY),
                Collections.singletonList(
                        OTHER_COLUMNS.get(column).getName() + " = '" + fname + "'"
                )), searchStmt);
    }


    private static ResultSet executeOrSelectUsingColumns(List<Integer> columns, List<String> values, Statement searchStmt) throws SQLException {
        List<String> conditions = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            conditions.add(OTHER_COLUMNS.get(columns.get(i)).getName() + " = " + SqlHelper.wrapWithQuote(SqlHelper.escape(values.get(i))));
        }
        return executeQuerySQL(SqlHelper.getSelectSql(
                        TABLE_NAME,
                        Collections.singletonList(PRIMARY_KEY),
                        conditions),
                searchStmt);
    }


    private static void executeUpdateSQL(String sql) throws SQLException {
        log.debug("Executing SQL: {}", sql);
        stmt.executeUpdate(sql);
    }

    private static ResultSet executeQuerySQL(String sql, Statement stmt) throws SQLException {
        log.debug("Executing SQL: {}", sql);
        return stmt.executeQuery(sql);
    }

    private static void checkEquality(ResultSet rs1, ResultSet rs2){
        try {
            while (rs1.next() && rs2.next()) {
                for (int i = 0; i < rs1.getMetaData().getColumnCount(); i++) {
                    String colName = rs1.getMetaData().getColumnName(i + 1);
                    String colValue1 = rs1.getString(colName);
                    String colValue2 = rs2.getString(colName);
                    log.debug("{} = {}", colName, colValue1);
                    log.debug("{} = {}", colName, colValue2);
                    if (!colValue1.equals(colValue2)) {
                        log.error("Column {} has different values: {} vs {}", colName, colValue1, colValue2);
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Error while checking equality", e);
        }
    }

    private static void initStmt() throws SQLException {
        stmt = conn.createStatement();
        searchStmt1 = conn.createStatement();
        searchStmt2 = conn.createStatement();
    }

    private static void closeConnections() throws SQLException {
        // STEP 4: Clean-up environment
        stmt.close();
        searchStmt1.close();
        searchStmt2.close();
        conn.close();
    }

    private static void setUpDbConnection() throws ClassNotFoundException, SQLException {
        // STEP 1: Register JDBC driver
        Class.forName(JDBC_DRIVER);

        // STEP 2: Open a connection
        System.out.println("Connecting to database...");
        conn = DriverManager.getConnection(DB_URL,USER,PASS);
    }
}
