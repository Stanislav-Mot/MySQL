import utils.DataOfDB;
import utils.Log;

import java.sql.*;

public class JDBCConnection extends DataOfDB {

    private static Connection connection = null;
    private static Statement statement = null;
    private static ResultSet resultSet = null;

    public static Connection connectToDB() {
        Log.info("Connect to DB");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            Log.info("Connection completed");
        } catch (ClassNotFoundException e) {
            Log.error(e.getMessage());
        } catch (SQLException ex) {
            Log.error(ex.getMessage());
        }
        return connection;
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
                Log.info("Closing connection completed");
            } catch (SQLException e) {
                Log.error(e.getMessage());
            }
        }
        if (statement != null) {
            try {
                statement.close();
                Log.info("Closing statement completed");
            } catch (SQLException e) {
                Log.error(e.getMessage());
            }
        }
        if (resultSet != null) {
            try {
                resultSet.close();
                Log.info("Closing resultSet completed");
            } catch (SQLException e) {
                Log.error(e.getMessage());
            }
        }
    }

    public static void createTable(String query) {
        try {
            statement = connectToDB().prepareStatement(query);
            statement.executeUpdate(query);
            Log.info("Table is created");
        } catch (SQLException s) {
            Log.error("Table isn't created: " + s.getMessage());
        }
    }

    public static void insertIntoTable(String query) {
        try {
            statement = connectToDB().createStatement();
            statement.executeUpdate(query);
            Log.info("Insertion into table is completed successfully");
        } catch (SQLException s) {
            Log.error("Insertion into table isn't completed successfully: " + s.getMessage());
        }
    }

    public static ResultSet selectFromTable(String query) {
        try {
            statement = connectToDB().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            resultSet = statement.executeQuery(query);
            resultSet.next();
        } catch (SQLException s) {
            Log.error(s.getMessage());
        }
        return resultSet;
    }

    public static void updateTable(String query) {
        try {
            statement = connectToDB().createStatement();
            statement.executeUpdate(query);
            Log.info("Update of table is completed successfully");
        } catch (SQLException s) {
            Log.error("Update of table isn't completed successfully: " + s.getMessage());
        }
    }

    public static void deleteFromTable(String query) {
        try {
            statement = connectToDB().createStatement();
            statement.executeUpdate(query);
            Log.info("Delete from table is completed successfully");
        } catch (SQLException s) {
            Log.error("Delete from table isn't completed successfully: " + s.getMessage());
        }
    }

    public static void dropTable(String tableName) {
        String  query= "DROP TABLE " + tableName;
        try {
            statement = connectToDB().createStatement();
            statement.executeUpdate(query);
            Log.info("Drop table is completed successfully");
        } catch (SQLException s) {
            Log.error("Drop table isn't completed successfully: " + s.getMessage());
        }
    }
}
