package store.db;

import org.apache.commons.lang3.StringUtils;
import javafx.util.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableHandler {

    private String tableName;
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet = null;

    public TableHandler(String tableName, Connection connection) {
        this.tableName = tableName;
        this.connection = connection;
    }

    private void close() throws SQLException {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }

            if (connection != null) {
                connection.close();
            }

            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException throwable) {
            throw throwable;
        }
    }


    public HashMap<String, String> get(List<String> selectionColumns,
                                       HashMap<String, String> whitelistQuery,
                                       HashMap<String, String> blacklistQuery) throws SQLException {

        String suffix = "";
        if (!(whitelistQuery.isEmpty() && blacklistQuery.isEmpty())) {
            List<String> whitelistValues = new ArrayList<>();
            List<String> blacklistValues = new ArrayList<>();

            for (Map.Entry<String, String> querySet : whitelistQuery.entrySet()) {
                whitelistValues.add(querySet.getKey() + "='" + querySet.getValue() + "'");
            }

            for (Map.Entry<String, String> querySet : blacklistQuery.entrySet()) {
                blacklistValues.add(querySet.getKey() + "<=>'" + querySet.getValue() + "'");
            }
            whitelistValues.addAll(blacklistValues);
            suffix += " WHERE " +
                    StringUtils.join(whitelistValues, " AND ");
        }

        String statement = "SELECT " + StringUtils.join(selectionColumns, ",") +
                " FROM " + tableName + suffix;
        System.out.println(" get | " + statement);

        try {
            preparedStatement = connection.prepareStatement(statement);
            resultSet = preparedStatement.executeQuery();

            // Convert the first row of the result into a hash map
            HashMap<String, String> result = new HashMap<>();
            if (resultSet.next()) {
                for (String column : selectionColumns) {
                    result.put(column, resultSet.getString(column));
                }
            }
            return result;
        } catch (SQLException throwable) {
            throw throwable;
        } finally {
            close();
        }
    }

    public List<HashMap<String, String>> getAll(List<String> selectionColumns,
                                                HashMap<String, String> whitelistQuery,
                                                List<Pair<String, String>> blacklistQuery) throws SQLException {

        if (whitelistQuery == null) {
            whitelistQuery = new HashMap<String, String>();
        }

        if (blacklistQuery == null) {
            blacklistQuery = new ArrayList<>();
        }

        String suffix = "";
        if (!(whitelistQuery.isEmpty() && blacklistQuery.isEmpty())) {
            List<String> whitelistValues = new ArrayList<>();
            List<String> blacklistValues = new ArrayList<>();

            for (Map.Entry<String, String> querySet : whitelistQuery.entrySet()) {
                whitelistValues.add(querySet.getKey() + "='" + querySet.getValue() + "'");
            }

            for (Pair<String, String> querySet : blacklistQuery) {
                blacklistValues.add(querySet.getKey() + "!='" + querySet.getValue() + "'");
            }
            whitelistValues.addAll(blacklistValues);
            suffix += " WHERE " +
                    StringUtils.join(whitelistValues, " AND ");
        }



        String statement = "SELECT " + StringUtils.join(selectionColumns, ",") +
                " FROM " + tableName + suffix;

        System.out.println(" getAll | " + statement);

        try {
            preparedStatement = connection.prepareStatement(statement);
            resultSet = preparedStatement.executeQuery();

            // Convert the first row of the result into a hash map
            List<HashMap<String, String>> result = new ArrayList<>();
            while (resultSet.next()) {
                HashMap<String, String> row = new HashMap<>();
                for (String column : selectionColumns) {
                    row.put(column, resultSet.getString(column));
                }
                result.add(row);
            }
            return result;
        } catch (SQLException throwable) {
            throw throwable;
        } finally {
            close();
        }
    }

    public int insert(HashMap<String, String> rowValuesMap) throws SQLException {
        List<String> values = new ArrayList<>();

        String keys = "";
        String placeholderValues = "";
        for (Map.Entry<String, String> querySet : rowValuesMap.entrySet()) {
            keys += querySet.getKey() + ",";
            placeholderValues += "?,";
            values.add(querySet.getValue());
        }
        if (!keys.isEmpty() && !placeholderValues.isEmpty()) {
            keys = keys.substring(0, keys.length() - 1);
            placeholderValues = placeholderValues.substring(0, placeholderValues.length() - 1);
        }

        String statement = "INSERT INTO " + tableName + "(" + keys + ") VALUES(" + placeholderValues + ")";
        System.out.println(" insert | " + statement);

        try {
            preparedStatement = connection.prepareStatement(statement);
            // TODO: replace all the values with placeholders

            for (int i = 0; i < values.size(); i++) {
                preparedStatement.setString((i + 1), values.get(i));
            }

            /*
             *  From: https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html#executeUpdate()
             *
             *  [1..] the row count for SQL Data Manipulation Language (DML) statements
             *  [0]   for SQL statements that return nothing
             */
            return preparedStatement.executeUpdate();
        } catch (SQLException throwable) {
            throw throwable;
        } finally {
            close();
        }
    }

    public int update(HashMap<String, String> rowValuesMap,
                      HashMap<String, String> whitelistQuery,
                      HashMap<String, String> blacklistQuery) throws Exception {

        List<String> values = new ArrayList<>();

        String placeholderValues = "";
        for (Map.Entry<String, String> querySet : rowValuesMap.entrySet()) {
            placeholderValues += querySet.getKey() + " = ?,";
            values.add(querySet.getValue());
        }
        if (!placeholderValues.isEmpty()) {
            placeholderValues = placeholderValues.substring(0, placeholderValues.length() - 1);
        }

        String suffix = "";
        if (!(whitelistQuery.isEmpty() && blacklistQuery.isEmpty())) {
            List<String> whitelistValues = new ArrayList<>();
            List<String> blacklistValues = new ArrayList<>();

            for (Map.Entry<String, String> querySet : whitelistQuery.entrySet()) {
                whitelistValues.add(querySet.getKey() + "='" + querySet.getValue() + "'");
            }

            for (Map.Entry<String, String> querySet : blacklistQuery.entrySet()) {
                blacklistValues.add(querySet.getKey() + "<=>'" + querySet.getValue() + "'");
            }
            whitelistValues.addAll(blacklistValues);
            suffix += " WHERE " +
                    StringUtils.join(whitelistValues, " AND ");
        } else {
            throw new Exception("Empty WHERE clause!");
        }

        String statement = "UPDATE " + tableName +
                " SET " + placeholderValues + suffix;
        System.out.println(" update | " + statement);

        try {
            preparedStatement = connection.prepareStatement(statement);
            // TODO: replace all the values with placeholders

            for (int i = 0; i < values.size(); i++) {
                preparedStatement.setString((i + 1), values.get(i));
            }
           /*
             *  From: https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html#executeUpdate()
             *
             *  [1..] the row count for SQL Data Manipulation Language (DML) statements
             *  [0]   for SQL statements that return nothing
             */
            return preparedStatement.executeUpdate();
        } catch (SQLException throwable) {
            throw throwable;
        } finally {
            close();
        }
    }

    public int delete(HashMap<String, String> whitelistQuery,
                      HashMap<String, String> blacklistQuery) throws Exception {


        String suffix = "";
        if (!(whitelistQuery.isEmpty() && blacklistQuery.isEmpty())) {
            List<String> whitelistValues = new ArrayList<>();
            List<String> blacklistValues = new ArrayList<>();

            for (Map.Entry<String, String> querySet : whitelistQuery.entrySet()) {
                whitelistValues.add(querySet.getKey() + "='" + querySet.getValue() + "'");
            }

            for (Map.Entry<String, String> querySet : blacklistQuery.entrySet()) {
                blacklistValues.add(querySet.getKey() + "<=>'" + querySet.getValue() + "'");
            }
            whitelistValues.addAll(blacklistValues);
            suffix += " WHERE " +
                    StringUtils.join(whitelistValues, " AND ");
        } else {
            throw new Exception("Empty WHERE clause!");
        }

        String statement = "DELETE FROM " + tableName + suffix;
        System.out.println(" delete | " + statement);

        try {
            preparedStatement = connection.prepareStatement(statement);

           /*
             *  From: https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html#executeUpdate()
             *
             *  [1..] the row count for SQL Data Manipulation Language (DML) statements
             *  [0]   for SQL statements that return nothing
             */
            return preparedStatement.executeUpdate();
        } catch (SQLException throwable) {
            throw throwable;
        } finally {
            close();
        }
    }
}
