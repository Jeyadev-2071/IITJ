package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {
    private static final String URL = "jdbc:redshift://redshift-cluster-1.cj78nttzipwp.us-east-1.redshift.amazonaws.com:5439/dev";
    private static final String USER = "awsuser";
    private static final String PASSWORD = "Jeyadev#1604";
    private static Connection connection;
    
    // Establish connection to the Redshift database
    public static Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                Class.forName("com.amazon.redshift.jdbc.Driver");
                connection = DriverManager.getConnection(URL, USER, PASSWORD);
                System.out.println("Connected to Redshift database.");
            } catch (ClassNotFoundException e) {
                System.err.println("JDBC Driver not found. Ensure the driver is in the classpath.");
                e.printStackTrace();
            }
        }
        return connection;
    }

    // Close the database connection
    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
                System.out.println("Connection closed.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
