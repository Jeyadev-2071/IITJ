package com.example;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class App {
    public static void main(String[] args) {
        try {
            Connection connection = DatabaseConnection.getConnection();
            QueryExecutor executor = new QueryExecutor(connection);
            // executor.drop("DEVELOPMENT");
            // executor.createdatabase("DEVELOPMENT");
            // executor.create();
            // executor.insert_data();
            // System.out.println("Running Query 1");
            // executor.executeQuery("SELECT C.c_custkey, O.o_orderkey, O.o_totalprice, O.o_orderdate From  customer C\r\n" + //
            //                     "LEFT JOIN orders O ON C.c_custkey = O.o_custkey\r\n" + //
            //                     "LEFT JOIN nation N ON C.c_nationkey = N.n_nationkey\r\n" + //
            //                     "LEFT JOIN region R ON N.n_regionkey = R.r_regionkey\r\n" + //
            //                     "WHERE R.r_name = 'AMERICA' AND O.o_orderdate IS NOT NULL\r\n" + //
            //                     "ORDER BY O.o_orderdate DESC\r\n" + //
            //                     "LIMIT 10;");
            // System.out.println("Running Query 2");
            // executor.executeQuery("WITH BASE_DATA AS(\r\n" + //
            //                     "SELECT * FROM customer WHERE c_mktsegment = \r\n" + //
            //                     "(SELECT c_mktsegment FROM customer\r\n" + //
            //                     "GROUP BY c_mktsegment\r\n" + //
            //                     "ORDER BY COUNT(*) DESC\r\n" + //
            //                     "LIMIT 1))\r\n" + //
            //                     "SELECT B.c_custkey, SUM(O.o_totalprice) AS Total_Spending FROM BASE_DATA B \r\n" + //
            //                     "LEFT JOIN orders O ON B.c_custkey = O.o_custkey\r\n" + //
            //                     "LEFT JOIN nation N ON B.c_nationkey = N.n_nationkey\r\n" + //
            //                     "LEFT JOIN region R ON N.n_regionkey = R.r_regionkey\r\n" + //
            //                     "WHERE R.r_name != 'EUROPE'\r\n" + //
            //                     "AND O.o_orderpriority = '1-URGENT' AND O.o_orderstatus = 'O'\r\n" + //
            //                     "GROUP BY B.c_custkey\r\n" + //
            //                     "ORDER BY Total_Spending DESC;");
            System.out.println("Running Query 3");
            executor.executeQuery("SELECT  O.o_orderpriority AS ORDER_PRIORITY, COUNT(L.l_linenumber) AS LINE_ITEM_NUMBER FROM orders O\r\n" + //
                                "LEFT JOIN lineitem L ON O.o_orderkey = L.l_orderkey\r\n" + //
                                "WHERE O.o_orderdate BETWEEN '1997-04-01' AND '2003-03-31'\r\n" + //
                                "GROUP BY O.o_orderpriority\r\n" + //
                                "ORDER BY O.o_orderpriority;");
            DatabaseConnection.closeConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
