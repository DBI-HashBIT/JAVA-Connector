import org.h2.table.Column;
import java.util.List;

public class SqlHelper {

    public static String getCreateTableSql(String tableName, Column primaryKey, List<Column> otherColumns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (");
        sb.append(primaryKey.getName()).append(" ").append(primaryKey.getType()).append(" AUTO_INCREMENT").append(" PRIMARY KEY").append(",");
        for (Column column : otherColumns) {
            sb.append(column.getName()).append(" ").append(column.getType()).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }

    public static String getDropTableSql(String tableName) {
        return "DROP TABLE IF EXISTS " + tableName;
    }

    public static String getCreateHashIndexSql(String indexName, String tableName, String columnName) {
        return "CREATE HASHBIT INDEX " + indexName + " on " + tableName + "(" + columnName + ")";
    }

    public static String getCreateHashIndexSql(String indexName, String tableName, String columnName, int bucketSize) {
        return "CREATE HASHBIT INDEX BUCKETS " + bucketSize + " " + indexName +   " on " + tableName + "(" + columnName + ")";
    }

    public static String getInsertSql(String tableName, List<Column> columns, List<String> values) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName).append(" (");
        for (Column column : columns) {
            sb.append(column.getName()).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(") VALUES (");
        for (String value : values) {
            sb.append(wrapWithQuote(escape(value))).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }

    public static String getSelectSql(String tableName, List<Column> columns, List<String> conditions) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (Column column : columns) {
            sb.append(column.getName()).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" FROM ").append(tableName);
        if (conditions != null && conditions.size() > 0) {
            sb.append(" WHERE ");
            for (String condition : conditions) {
                sb.append(condition).append(" AND ");
            }
            sb.delete(sb.length() - 5, sb.length());
        }
        return sb.toString();
    }

    public static String escape(String str) {
        return str.replace("'", "''");
    }

    public static String wrapWithQuote(String str) {
        return "'" + str + "'";
    }




}
