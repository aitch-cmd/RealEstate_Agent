import sys
from db.connection import MongoDBClient
from scraping.message_parser import UserMessageParser
from pymongo.errors import OperationFailure

class MongoDBFilter:
    def __init__(self, collection_name: str="tulire_listings"):
        """Initialize DB connection and ensure indexes exist"""
        self.db_client = MongoDBClient(database_name="rental_database")
        self.collection = self.db_client.database[collection_name]

        try:
            self.collection.create_index([("title", "text")]) 
            self.collection.create_index([("rent_price", 1)])
            print("Indexes ensured: text on 'title', ascending on 'rent_price'")
        except OperationFailure as e:
            print(f"Index creation failed: {e}")

    def build_query(self, parsed_message: dict) -> dict:
        """
        Build a MongoDB query using extracted info from user message.
        Uses $text for title search (case-insensitive) and numeric filter for rent_price.
        """
        query = {}

        # Location/title search using $text
        if parsed_message.get("location"):
            query["$text"] = {"$search": parsed_message["location"]}

        # Rent price filter
        if parsed_message.get("price"):
            try:
                price_val = int(parsed_message["price"])
                query["rent_price"] = {"$lte": price_val}
            except ValueError:
                print(f"Invalid price value: {parsed_message['price']}")

        return query
    
    def search_rentals(self, user_message: str):
        """
        Parse user message, build query, and fetch matching rentals.
        """
        parser = UserMessageParser()
        parsed = parser.extract(user_message)

        print("\n📝 Parsed message:", parsed)

        query = self.build_query(parsed)
        print("🔎 MongoDB Query:", query)

        results = list(self.collection.find(query))

        if not results:
            print("No listings found for this query.")

        return results

if __name__ == "__main__":
    executor = MongoDBFilter()

    # Example messages for testing
    test_messages = [
        "Looking for a 1BHK flat in South Orange under 2200"
    ]

    for msg in test_messages:
        print("\n💬 User Input:", msg)
        listings = executor.search_rentals(msg)

        print("📌 Matching Listings:")
        for l in listings:
            print(l)