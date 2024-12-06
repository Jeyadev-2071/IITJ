import org.bson.Document;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.include;

import java.util.Arrays;

import static com.mongodb.client.model.Projections.fields;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

/**
 * Example on how to use the Mongo API directly.
 */
public class ExampleMongo {
	/**
	 * Main method
	 * 
	 * @param args
	 *             no arg required
	 */
	public static void main(String[] args) {
		// TODO: Replace with your cluster info
		String url = "";

		try (
			// Provide connection information to MongoDB server
			MongoClient mongoClient = MongoClients.create(url);) {
			// Provide database information to connect to
			MongoDatabase db = mongoClient.getDatabase("mydb");

			// Add ten items to a collection
			MongoCollection<Document> collection = db.getCollection("testdata");

			for (int i = 1; i <= 10; i++) {
				collection.insertOne(new Document()
						.append("num", i)
						.append("str", "str" + i)
						.append("vals", Arrays.asList(i, i * 10, i * 20)));
			}

			// Get a list of the collections in this database and print them out
			System.out.println("List of collections: ");
			MongoIterable<String> collectionNames = db.listCollectionNames();
			for (String s : collectionNames) {
				System.out.println("\nCollection: " + s);

				// Print out first 5 documents of the collection
				if (s.contains("system"))
					continue; // Do not query any system collections
				System.out.println("Documents:\n");
				MongoCollection<Document> col = db.getCollection(s);
				MongoCursor<Document> cursor = col.find().iterator();
				for (int i = 0; i < 5 && cursor.hasNext(); i++) {
					Document doc = cursor.next();
					System.out.println(doc);
				}
			}

			// Sample query #2 equivalent SQL: SELECT num, str FROM testdata where num < 3
			System.out.println("\n\nSample query with output:");
			MongoCollection<Document> col = db.getCollection("testdata");
			Document query = new Document("num", new Document("$lt", 3));
			MongoCursor<Document> cursor = col.find(query).projection(fields(include("num", "str"), excludeId())).iterator();
			while (cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println(doc);
			}
			cursor.close();

			System.out.println("\nFINISHED!");
		} catch (Exception ex) {
			System.out.println("Exception: " + ex);
			ex.printStackTrace();
		}
	}
}
