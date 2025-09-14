from scraping.message_parser import UserMessageParser
from retrieval.mgdb_filter import MongoDBFilter
from retrieval.primary_ranker import PrimaryHybridReranker
from retrieval.secondary_ranker import SecondaryHybridReranker
from retrieval.cross_encoder_reranker import CrossEncoderReranker
from typing import List, Dict
from rental_agent import RentalAgent

class RentalPipeline:
    def __init__(self):
        self.message_parser = UserMessageParser()
        self.mongo_db_filter = MongoDBFilter()
        self.primary_reranker = PrimaryHybridReranker()
        self.secondary_reranker = SecondaryHybridReranker()
        self.cross_encoder_reranker = CrossEncoderReranker()
        self.agent = RentalAgent()

    def process_user_message(self, user_message: str) -> List[Dict]:
        # Step 1: Parse user message
        parsed_message = self.message_parser.extract(user_message)

        # Step 2: MongoDB query retrieval
        initial_listings = self.mongo_db_filter.search_rentals(parsed_message)

        # Step 3: Ranking on the basis of number of listings and soft preferences
        if initial_listings is None or len(initial_listings) == 0:
            ranked_listings = []
        elif parsed_message.get("rag_content") is None:
            ranked_listings = initial_listings
        elif len(initial_listings) < 5 and parsed_message.get("rag_content") is not None:
            ranked_listings = self.primary_reranker.rerank(parsed_message, initial_listings)
        elif len(initial_listings) >= 5 and parsed_message.get("rag_content") is not None:
            ranked_listings = self.secondary_reranker.rerank(parsed_message, initial_listings)
        
        # Step 4: Rerank listings using cross-encoder 
        reranked_listings = self.cross_encoder_reranker.rerank(parsed_message, ranked_listings)

        # Step 5 Use the agent to finalize the listings (mocked here)
        final_listings = self.agent.finalize_listings(reranked_listings)

        return final_listings