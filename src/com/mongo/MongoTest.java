package com.mongo;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static com.mongodb.client.model.Filters.*;
import static java.util.Arrays.asList;

public class MongoTest {
    // Connect to MongoDB
    static MongoClient mongoClient = new MongoClient();
    static MongoDatabase db = mongoClient.getDatabase("test");

    public static void main(String[] args) {
//        try {
//            // Insert a Document
//            InsertADocument();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }

        FindOrQueryDataWithJavaDriver();
    }

    private static void InsertADocument() throws ParseException {
        // To specify a document, use the org.bson.Document class.
        // The method does not return a result.
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
        db.getCollection("restaurants").insertOne(
                new Document("address",
                        new Document()
                                .append("street", "2 Avenue")
                                .append("zipcode", "10075")
                                .append("building", "1480")
                                .append("coord", asList(-73.9557413, 40.7720266)))
                        .append("borough", "Manhattan")
                        .append("cuisine", "Italian")
                        .append("grades", asList(
                                new Document()
                                        .append("date", format.parse("2014-10-01T00:00:00Z"))
                                        .append("grade", "A")
                                        .append("score", 11),
                                new Document()
                                        .append("date", format.parse("2014-01-16T00:00:00Z"))
                                        .append("grade", "B")
                                        .append("score", 17)))
                        .append("name", "Vella")
                        .append("restaurant_id", "41704620"));
    }

    private static void FindOrQueryDataWithJavaDriver() {
        // Query for All Documents in a Collection
        System.out.println("Query for All Documents in a Collection: ");
        // To return all documents in a collection, call the find method without a criteria document. For example, the
        // following operation queries for all documents in the restaurants collection.
        FindIterable<Document> iterable = db.getCollection("restaurants").find();
        // Iterate the results and apply a block to each resulting document.
        // The result set contains all documents in the restaurants collection.
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });

        System.out.println("\n==========================================\n");
        System.out.println("\n==========================================\n");

        // Specify Equality Conditions

        // Query by a Top Level Field
        // The following operation finds documents whose borough field equals "Manhattan".
        System.out.println("Query by a Top Level Field:");
        iterable = db.getCollection("restaurants").find(
                new Document("borough", "Manhattan"));
        // Iterate the results and apply a block to each resulting document.
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        System.out.println("--------------------------------------------");
        // Using the static Filters helper(s), you can also specify the query as follows:
        for (Document document : db.getCollection("restaurants").find(eq("borough", "Manhattan"))) {
            System.out.println(document);
        }

        System.out.println("\n==========================================\n");

        // Query by a Field in an Embedded Document
        // To specify a condition on a field within an embedded document, use the dot notation. Dot notation requires
        // quotes around the whole dotted field name. The following operation specifies an equality condition on the
        // zipcode field in the address embedded document.
        System.out.println("Query by a Field in an Embedded Document:");
        iterable = db.getCollection("restaurants").find(
                new Document("address.zipcode", "10075"));
        // Iterate the results and apply a block to each resulting document.
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        System.out.println("--------------------------------------------");
        // Using the static Filters helper(s), you can also specify the query as follows:
        for (Document restaurants : db.getCollection("restaurants").find(eq("address.zipcode", "10075"))) {
            System.out.println(restaurants);
        }

        System.out.println("\n==========================================\n");

        // Query by a Field in an Array
        // The grades array contains embedded documents as its elements. To specify a condition on a field in these
        // documents, use the dot notation. Dot notation requires quotes around the whole dotted field name. The
        // following queries for documents whose grades array contains an embedded document with a field grade equal
        // to "B".
        System.out.println("Query by a Field in an Array:");
        iterable = db.getCollection("restaurants").find(
                new Document("grades.grade", "B"));
        // Iterate the results and apply a block to each resulting document.
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        System.out.println("--------------------------------------------");
        // Using the static Filters helper(s), you can also specify the query as follows:
        for (Document document : db.getCollection("restaurants").find(eq("grades.grade", "B"))) {
            System.out.println(document);
        }

        System.out.println("\n==========================================\n");
        System.out.println("\n==========================================\n");

        // Specify Conditions with Operators

        // Greater Than Operator ($gt)
        // Query for documents whose grades array contains an embedded document with a field score greater than 30.
        System.out.println("Greater Than Operator ($gt):");
        iterable = db.getCollection("restaurants").find(
                new Document("grades.score", new Document("$gt", 30)));
        // Iterate the results and apply a block to each resulting document.
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        System.out.println("--------------------------------------------");
        // Using the static Filters helper(s), you can also specify the query as follows:
        for (Document restaurants : db.getCollection("restaurants").find(gt("grades.score", 30))) {
            System.out.println(restaurants);
        }

        System.out.println("\n==========================================\n");

        // Less Than Operator ($lt)
        // Query for documents whose grades array contains an embedded document with a field score less than 10.
        System.out.println("Less Than Operator ($lt):");
        iterable = db.getCollection("restaurants").find(
                new Document("grades.score", new Document("$lt", 10)));
        // Iterate the results and apply a block to each resulting document.
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        System.out.println("--------------------------------------------");
        // Using the static Filters helper(s), you can also specify the query as follows:
        for (Document restaurants : db.getCollection("restaurants").find(lt("grades.score", 10))) {
            System.out.println(restaurants);
        }

        System.out.println("\n==========================================\n");

        // Combine Conditions

        // Logical AND
        // You can specify a logical conjunction (AND) for multiple query conditions by appending conditions to the
        // query document. See the append method in the org.bson.Document class.
        System.out.println("Logical AND:");
        iterable = db.getCollection("restaurants").find(
                new Document("cuisine", "Italian").append("address.zipcode", "10075"));
        // The result set includes only the documents that matched all specified criteria.
        // Iterate the results and apply a block to each resulting document.
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        System.out.println("--------------------------------------------");
        // Using the static Filters helper(s), you can also specify the query as follows:
        for (Document document : db.getCollection("restaurants").find(and(eq("cuisine", "Italian"), eq("address.zipcode", "10075")))) {
            System.out.println(document);
        }
    }
}
