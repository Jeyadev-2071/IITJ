package com.example;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExampleMongo {

    public static void main(String[] args) {
        // MongoDB connection string
        String connectionString = "mongodb+srv://g23ai2071:Jeyadev1604@cluster-jd.tsrhf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-jd";

        // MongoDB database name
        String databaseName = "assignment_7_JD";

        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);

            // Ensure collections exist and load data
            // createAndLoadCollections(database);
            // System.out.println("\nData loading completed successfully!");
            // int customerId = 1; // Replace with the desired customer ID
            // String customerName = query1(database, customerId);
            // if (customerName != null) {
            //     System.out.println("Customer Name: " + customerName);
            // } else {
            //     System.out.println("Customer with ID " + customerId + " not found.");
            // }

            // int orderId = 1; 
            // String orderDate = query2(database, orderId);
            // if (orderDate != null) {
            //     System.out.println("Order Date: " + orderDate);
            // } else {
            //     System.out.println("Order with ID " + orderId + " not found.");
            // }

            // String Nest_orderDate = query2Nest(database, orderId);
            // if (Nest_orderDate != null) {
            //     System.out.println("Order Date from custorders: " + Nest_orderDate);
            // } else {
            //     System.out.println("Order with ID " + orderId + " not found in custorders.");
            // }

            long totalOrders = query3(database);
            System.out.println("Total Number of Orders: " + totalOrders);
            
            long Nest_totalOrders = query3Nest(database);
            System.out.println("Total Number of Orders in custorders: " + Nest_totalOrders);
            
            // List<Document> topCustomers = query4(database);
            // System.out.println("Top 5 Customers by Total Order Amount:");
            // for (Document customer : topCustomers) {
            //     System.out.println(customer.toJson());
            // }
            // List<Document> Nest_topCustomers = query4Nest(database);
            // System.out.println("Top 5 Customers by Total Order Amount (custorders):");
            // for (Document customer : Nest_topCustomers) {
            //     System.out.println(customer.toJson());
            // }

        } catch (Exception ex) {
            System.err.println("An error occurred:");
            ex.printStackTrace();
        }
    }

    /**
     * Ensures the collections exist and loads data into them.
     *
     * @param database MongoDB database instance
     */
    public static void createAndLoadCollections(MongoDatabase database) {
        // File paths
        String customerFilePath = "data/customer.tbl";
        String ordersFilePath = "data/order.tbl";

        // Ensure collections are created
        MongoCollection<Document> customerCollection = database.getCollection("customer");
        MongoCollection<Document> ordersCollection = database.getCollection("orders");

        // Load data into collections
        System.out.println("Loading customer data...");
        loadPipeDelimitedData(customerFilePath, customerCollection, new String[]{
                "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"
        });

        System.out.println("Loading orders data...");
        loadPipeDelimitedData(ordersFilePath, ordersCollection, new String[]{
                "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
                "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"
        });
        loadNest(database,customerCollection,ordersCollection);
    }
    /**
     * Loads pipe-delimited data from a file into a MongoDB collection.
     *
     * @param filePath   Path to the pipe-delimited file
     * @param collection MongoDB collection to load data into
     * @param headers    Array of field names corresponding to the file structure
     */
    private static void loadPipeDelimitedData(String filePath, MongoCollection<Document> collection, String[] headers) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            List<Document> documents = new ArrayList<>();

            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\|"); // Pipe-delimited
                Document document = new Document();
                for (int i = 0; i < headers.length; i++) {
                    document.append(headers[i], parseValue(values[i]));
                }
                documents.add(document);

                // Insert in batches of 1000 for performance
                if (documents.size() >= 1000) {
                    collection.insertMany(documents);
                    documents.clear();
                }
            }

            // Insert remaining documents
            if (!documents.isEmpty()) {
                collection.insertMany(documents);
            }

            System.out.println("Loaded data into collection: " + collection.getNamespace().getCollectionName());
        } catch (Exception e) {
            System.err.println("Error loading data into collection: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Parses a string value to handle nulls or type conversions.
     *
     * @param value Raw string value
     * @return Parsed object (String, Integer, Double, or null)
     */
    private static Object parseValue(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Integer.parseInt(value);
            }
        } catch (NumberFormatException e) {
            return value.trim(); // Default to String
        }
    }

    /**
     * Loads customer and orders data into a nested collection 'custorders'.
     *
     * @param database MongoDB database instance
     */
    public static void loadNest(MongoDatabase database,MongoCollection<Document> customerCollection,MongoCollection<Document> ordersCollection) {
       

        // Target collection
        MongoCollection<Document> custOrdersCollection = database.getCollection("custorders");

        // Clear the target collection if it exists
        custOrdersCollection.drop();

        System.out.println("Creating nested collection 'custorders'...");

        // Iterate through each customer
        try (MongoCursor<Document> customerCursor = customerCollection.find().iterator()) {
            List<Document> nestedDocuments = new ArrayList<>();
            while (customerCursor.hasNext()) {
                Document customer = customerCursor.next();
                int customerId = customer.getInteger("c_custkey");

                // Find all orders for the current customer
                List<Document> orders = ordersCollection.find(new Document("o_custkey", customerId)).into(new ArrayList<>());

                // Embed the orders in the customer document
                customer.append("orders", orders);

                // Add the nested document to the list
                nestedDocuments.add(customer);

                // Insert in batches for performance
                if (nestedDocuments.size() >= 1000) {
                    custOrdersCollection.insertMany(nestedDocuments);
                    nestedDocuments.clear();
                }
            }

            // Insert remaining nested documents
            if (!nestedDocuments.isEmpty()) {
                custOrdersCollection.insertMany(nestedDocuments);
            }

            System.out.println("Data loaded into 'custorders' collection.");
        } catch (Exception e) {
            System.err.println("Error loading nested data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Returns the customer name given a customer ID using the customer collection.
     *
     * @param database    MongoDB database instance
     * @param customerId  The ID of the customer to query
     * @return The customer's name, or null if no customer is found
     */
    public static String query1(MongoDatabase database, int customerId) {
        // Access the customer collection
        MongoCollection<Document> customerCollection = database.getCollection("customer");

        // Query the collection for the given customer ID
        Document customer = customerCollection.find(eq("c_custkey", customerId)).first();

        // Return the customer name if found, otherwise return null
        if (customer != null) {
            return customer.getString("c_name");
        }
        return null;
    }

     /**
     * Returns the order date for a given order ID using the orders collection.
     *
     * @param database MongoDB database instance
     * @param orderId  The ID of the order to query
     * @return The order date, or null if no order is found
     */
    public static String query2(MongoDatabase database, int orderId) {
        // Access the orders collection
        MongoCollection<Document> ordersCollection = database.getCollection("orders");

        // Query the collection for the given order ID
        Document order = ordersCollection.find(eq("o_orderkey", orderId)).first();

        // Return the order date if found, otherwise return null
        if (order != null) {
            return order.getString("o_orderdate");
        }
        return null;
    }

    /**
     * Returns the order date for a given order ID using the custorders collection.
     *
     * @param database MongoDB database instance
     * @param orderId  The ID of the order to query
     * @return The order date, or null if no order is found
     */
    public static String query2Nest(MongoDatabase database, int orderId) {
        // Access the custorders collection
        MongoCollection<Document> custOrdersCollection = database.getCollection("custorders");

        // Iterate through each document in custorders
        for (Document customer : custOrdersCollection.find()) {
            // Get the list of orders embedded in the customer document
            List<Document> orders = customer.getList("orders", Document.class);

            // Search for the order with the given order ID
            for (Document order : orders) {
                if (order.getInteger("o_orderkey") == orderId) {
                    return order.getString("o_orderdate"); // Return the order date
                }
            }
        }
        return null;
    }

    /**
     * Returns the total number of orders using the orders collection.
     *
     * @param database MongoDB database instance
     * @return The total number of orders
     */
    public static long query3(MongoDatabase database) {
        // Access the orders collection
        MongoCollection<Document> ordersCollection = database.getCollection("orders");

        // Use countDocuments() to get the total number of orders
        return ordersCollection.countDocuments();
    }

    /**
     * Returns the total number of orders using the custorders collection.
     *
     * @param database MongoDB database instance
     * @return The total number of orders
     */
    public static long query3Nest(MongoDatabase database) {
        // Access the custorders collection
        MongoCollection<Document> custOrdersCollection = database.getCollection("custorders");

        long totalOrders = 0;

        // Iterate through each document in custorders
        for (Document customer : custOrdersCollection.find()) {
            // Get the list of orders embedded in the customer document
            List<Document> orders = customer.getList("orders", Document.class);
            if (orders != null) {
                totalOrders += orders.size(); // Add the number of orders for the customer
            }
        }
        return totalOrders;
    }

        /**
     * Returns the top 5 customers based on total order amount using the customer and orders collections.
     *
     * @param database MongoDB database instance
     * @return A list of top 5 customers as documents, including their total order amount
     */
    public static List<Document> query4(MongoDatabase database) {
        // Access the customer and orders collections
        MongoCollection<Document> customerCollection = database.getCollection("customer");
        MongoCollection<Document> ordersCollection = database.getCollection("orders");
        // Map to store total order amount by customer ID
        Map<Integer, Double> customerOrderTotals = new HashMap<>();
        // Iterate through all orders to calculate total order amount for each customer
        try (MongoCursor<Document> ordersCursor = ordersCollection.find().iterator()) {
            while (ordersCursor.hasNext()) {
                Document order = ordersCursor.next();
                int customerId = order.getInteger("o_custkey");
                double orderAmount = order.getDouble("o_totalprice");

                customerOrderTotals.put(customerId, customerOrderTotals.getOrDefault(customerId, 0.0) + orderAmount);
            }
        }
        // Retrieve customer details and include total order amount
        List<Document> customersWithTotals = customerCollection.find()
            .map(customer -> {
                int customerId = customer.getInteger("c_custkey");
                double totalOrderAmount = customerOrderTotals.getOrDefault(customerId, 0.0);
                customer.append("total_order_amount", totalOrderAmount);
                return customer;
            })
            .into(new java.util.ArrayList<>());
        // Sort customers by total order amount in descending order and return the top 5
        return customersWithTotals.stream()
            .sorted((c1, c2) -> Double.compare(c2.getDouble("total_order_amount"), c1.getDouble("total_order_amount")))
            .limit(5)
            .collect(Collectors.toList());
    }

     /**
     * Returns the top 5 customers based on total order amount using the custorders collection.
     * Only includes customer details and the total amount spent, without order details.
     *
     * @param database MongoDB database instance
     * @return A list of top 5 customers as documents, including their total order amount
     */
    public static List<Document> query4Nest(MongoDatabase database) {
        // Access the custorders collection
        MongoCollection<Document> custOrdersCollection = database.getCollection("custorders");

        // Retrieve all customers, calculate total order amount, and sort by total amount
        return custOrdersCollection.find()
            .map(customer -> {
                // Calculate the total order amount from the embedded orders array
                List<Document> orders = customer.getList("orders", Document.class);
                double totalOrderAmount = orders.stream()
                        .mapToDouble(order -> order.getDouble("o_totalprice"))
                        .sum();

                // Create a new document with customer details and total amount spent
                return new Document("c_custkey", customer.getInteger("c_custkey"))
                        .append("c_name", customer.getString("c_name"))
                        .append("c_address", customer.getString("c_address"))
                        .append("c_phone", customer.getString("c_phone"))
                        .append("c_acctbal", customer.getDouble("c_acctbal"))
                        .append("total_order_amount", totalOrderAmount);
            })
            .into(new java.util.ArrayList<>()) // Collect into a list
            .stream() // Stream the list for sorting
            .sorted((c1, c2) -> Double.compare(c2.getDouble("total_order_amount"), c1.getDouble("total_order_amount"))) // Sort by total order amount descending
            .limit(5) // Limit to top 5
            .collect(Collectors.toList()); // Collect the top 5 results
    }

}
