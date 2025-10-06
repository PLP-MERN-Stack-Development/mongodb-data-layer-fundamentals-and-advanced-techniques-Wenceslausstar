// queries.js
// MongoDB CRUD + Advanced Queries + Aggregations + Indexing

const { MongoClient } = require("mongodb");

// Connection URI (replace with your MongoDB Atlas URI if changed)
const uri =
  "mongodb+srv://wesleywenceslaus07_db_user:bookstore@bookstore.mnoggba.mongodb.net/?retryWrites=true&w=majority&appName=bookstore";

const dbName = "plp_bookstore";
const collectionName = "books";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB Atlas");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // ======================
    // TASK 2: BASIC CRUD OPERATIONS
    // ======================

    // 1. Find all books in a specific genre
    const genre = "Fiction";
    const fictionBooks = await collection.find({ genre }).toArray();
    console.log(
      `\nBooks in genre "${genre}":`,
      fictionBooks.map((b) => b.title)
    );

    // 2. Find books published after a certain year
    const year = 2000;
    const recentBooks = await collection
      .find({ published_year: { $gt: year } })
      .toArray();
    console.log(
      `\nBooks published after ${year}:`,
      recentBooks.map((b) => b.title)
    );

    // 3. Find books by a specific author
    const authorName = "George Orwell";
    const authorBooks = await collection.find({ author: authorName }).toArray();
    console.log(
      `\nBooks by ${authorName}:`,
      authorBooks.map((b) => b.title)
    );

    // 4. Update the price of a specific book
    const updateTitle = "The Great Gatsby";
    const newPrice = 15.99;
    const updateResult = await collection.updateOne(
      { title: updateTitle },
      { $set: { price: newPrice } }
    );
    console.log(
      `\nUpdated price of "${updateTitle}":`,
      updateResult.modifiedCount > 0 ? "Success" : "No match found"
    );

    // 5. Delete a book by its title
    const deleteTitle = "Moby Dick";
    const deleteResult = await collection.deleteOne({ title: deleteTitle });
    console.log(
      `\nDeleted "${deleteTitle}":`,
      deleteResult.deletedCount > 0 ? "Success" : "No match found"
    );

    // ======================
    // TASK 3: ADVANCED QUERIES
    // ======================

    // 1. Find books that are in stock and published after 2010
    const modernStockBooks = await collection
      .find({ in_stock: true, published_year: { $gt: 2010 } })
      .toArray();
    console.log(
      "\nBooks in stock and published after 2010:",
      modernStockBooks.map((b) => b.title)
    );

    // 2. Projection: only title, author, and price
    const projectionBooks = await collection
      .find({}, { projection: { _id: 0, title: 1, author: 1, price: 1 } })
      .toArray();
    console.log("\nProjection (title, author, price):", projectionBooks);

    // 3. Sorting by price ascending
    const sortedAsc = await collection.find({}).sort({ price: 1 }).toArray();
    console.log(
      "\nSorted by price (ascending):",
      sortedAsc.map((b) => `${b.title} - $${b.price}`)
    );

    // 4. Sorting by price descending
    const sortedDesc = await collection.find({}).sort({ price: -1 }).toArray();
    console.log(
      "\nSorted by price (descending):",
      sortedDesc.map((b) => `${b.title} - $${b.price}`)
    );

    // 5. Pagination (5 books per page)
    const pageSize = 5;
    const page = 1; // Change this number to test different pages
    const paginatedBooks = await collection
      .find({})
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .toArray();
    console.log(
      `\nPage ${page} (5 books per page):`,
      paginatedBooks.map((b) => b.title)
    );

    // ======================
    // TASK 4: AGGREGATION PIPELINES
    // ======================

    // 1. Average price of books by genre
    const avgPriceByGenre = await collection
      .aggregate([
        { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
        { $sort: { avgPrice: -1 } },
      ])
      .toArray();
    console.log("\nAverage price by genre:", avgPriceByGenre);

    // 2. Author with the most books
    const topAuthor = await collection
      .aggregate([
        { $group: { _id: "$author", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 1 },
      ])
      .toArray();
    console.log("\nAuthor with the most books:", topAuthor);

    // 3. Group books by publication decade
    const booksByDecade = await collection
      .aggregate([
        {
          $project: {
            decade: {
              $concat: [
                {
                  $toString: {
                    $subtract: [
                      "$published_year",
                      { $mod: ["$published_year", 10] },
                    ],
                  },
                },
                "s",
              ],
            },
          },
        },
        { $group: { _id: "$decade", count: { $sum: 1 } } },
        { $sort: { _id: 1 } },
      ])
      .toArray();
    console.log("\nBooks grouped by decade:", booksByDecade);

    // ======================
    // TASK 5: INDEXING
    // ======================

    // Create an index on title
    await collection.createIndex({ title: 1 });
    console.log("\nCreated index on 'title'");

    // Create a compound index on author and published_year
    await collection.createIndex({ author: 1, published_year: -1 });
    console.log("Created compound index on 'author' and 'published_year'");

    // Demonstrate performance improvement with explain()
    const explainResult = await collection
      .find({ title: "The Great Gatsby" })
      .explain("executionStats");
    console.log(
      "\nQuery performance (execution time):",
      explainResult.executionStats.executionTimeMillis,
      "ms"
    );
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log("Connection closed");
  }
}

// Run all queries
runQueries();
