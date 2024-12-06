package com.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryExecutor {
    private final Connection connection;

    public QueryExecutor(Connection connection) {
        this.connection = connection;
    }

    public void drop(String databaseName) throws SQLException {
        System.out.println("Dropping all tables...");
    
        // Step 1: Drop all tables
        String query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';";
    
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
    
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                String dropSQL = "DROP TABLE IF EXISTS " + tableName + " CASCADE;";
                try (Statement dropStmt = connection.createStatement()) {
                    dropStmt.executeUpdate(dropSQL);
                    System.out.println("Dropped table: " + tableName);
                } catch (SQLException e) {
                    System.err.println("Error dropping table: " + tableName);
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving table names.");
            e.printStackTrace();
        }
    
        System.out.println("All tables dropped successfully.");
    
        // Step 2: Drop the database
        System.out.println("Dropping database: " + databaseName);
    
        String dropDatabaseSQL = "DROP DATABASE " + databaseName + ";";
    
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(dropDatabaseSQL);
            System.out.println("Database '" + databaseName + "' dropped successfully.");
        } catch (SQLException e) {
            System.err.println("Error dropping database: " + e.getMessage());
        }
    }

    // Create the database if it does not already exist
    public void createdatabase(String databaseName) throws SQLException {
        System.out.println("Ensuring database: " + databaseName);
    
        // Check if the database exists
        String checkSQL = "SELECT datname FROM pg_database WHERE datname = '" + databaseName + "';";
        boolean databaseExists = false;
    
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(checkSQL)) {
            if (rs.next()) {
                databaseExists = true;
            }
        }
    
        if (databaseExists) {
            System.out.println("Database '" + databaseName + "' already exists. Skipping creation.");
        } else {
            // Create the database if it doesn't exist
            String createSQL = "CREATE DATABASE " + databaseName + ";";
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(createSQL);
                System.out.println("Database '" + databaseName + "' created successfully.");
            } catch (SQLException e) {
                System.err.println("Error creating database: " + e.getMessage());
                throw e;
            }
        }
    }

    // Create the database and tables using the provided DDL files
    public void create() throws SQLException, IOException {
        System.out.println("Creating tables...");
        executeDDlFromFile("ddl_data/tpch_create.sql");

        System.out.println("Tables created successfully.");
    }


    // Insert the standard TPC-H data
    public void insert_data() throws SQLException, IOException {
        System.out.println("Inserting TPC-H data...");
    
        // List of SQL files containing insert statements
        String[] dataFiles = {
                "ddl_data/customer.sql",
                "ddl_data/lineitem.sql",
                "ddl_data/nation.sql",
                "ddl_data/orders.sql",
                "ddl_data/part.sql",
                "ddl_data/partsupp.sql",
                "ddl_data/region.sql",
                "ddl_data/supplier.sql"
        };
    
        // Execute each SQL file
        for (String dataFile : dataFiles) {
            executeSQLFromFile(dataFile);
        }
    
        System.out.println("TPC-H data inserted successfully.");
    }

    public void executeQuery(String sql) throws SQLException {
    System.out.println("Executing Query...");
    try (PreparedStatement stmt = connection.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {

        // Get metadata to dynamically retrieve column information
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Print column names
        System.out.print("| ");
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(metaData.getColumnName(i) + " | ");
        }
        System.out.println();

        // Print each row
        while (rs.next()) {
            System.out.print("| ");
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getObject(i) + " | ");
            }
            System.out.println();
        }
    } catch (SQLException e) {
        System.err.println("Error executing query: " + e.getMessage());
        throw e;
        }
    }

    private void executeDDlFromFile(String filePath) throws IOException, SQLException {
        StringBuilder sqlBuilder = new StringBuilder();
    
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sqlBuilder.append(line).append("\n");
            }
        }
    
        try (Statement stmt = connection.createStatement()) {
            String[] sqlCommands = sqlBuilder.toString().split(";"); // Split commands by semicolon
            for (String sql : sqlCommands) {
                if (!sql.trim().isEmpty()) { // Avoid empty statements
                    stmt.execute(sql.trim());
                }
            }
        }
    }
    private void executeSQLFromFile(String filePath) throws IOException, SQLException {
        System.out.println("Executing SQL from file: " + filePath);
    
        StringBuilder sqlBuilder = new StringBuilder();
        int commandCount = 0;
    
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("--")) {
                    // Skip empty lines or comments
                    continue;
                }
                sqlBuilder.append(line);
                if (line.endsWith(";")) {
                    // Execute the SQL command when a semicolon is encountered
                    String sqlCommand = sqlBuilder.toString();
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute(sqlCommand);
                        commandCount++;
                        System.out.println("Executed SQL command: ");
                    } catch (SQLException e) {
                        System.err.println("Error executing SQL command: ");
                        e.printStackTrace();
                    }
                    sqlBuilder.setLength(0); // Clear the builder for the next command
                } else {
                    // Continue building the SQL command
                    sqlBuilder.append(" ");
                }
            }
        }
    
        System.out.println("Executed " + commandCount + " SQL commands from file: " + filePath);
    }
}