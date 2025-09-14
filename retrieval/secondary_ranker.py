from sentence_transformers import SentenceTransformer, util
from typing import List, Dict
from retrieval.primary_ranker import PrimaryHybridReranker

class SecondaryHybridReranker:
    """
    Hybrid reranker class for listings (used when the number of listings > 4). 
    Combines embedding-based semantic similarity with user preference keyword matching.
    """

    CONFIG = PrimaryHybridReranker.load_params("params.yaml")["secondary_ranker"]

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
                 alpha: float = CONFIG["alpha"], beta: float = CONFIG["beta"]):
        """
        Initialize the reranker.
        Args:
            model_name: Pretrained SentenceTransformer model for embedding computation.
            alpha: Weight for embedding similarity score.
            beta: Weight for preference keyword overlap score.
        """
        self.embedder = SentenceTransformer(model_name)  # Load embedding model
        self.alpha = alpha  # Weight for semantic similarity
        self.beta = beta    # Weight for keyword preference score

    def build_listing_text(self, c: Dict) -> str:
        """
        Convert a listing dictionary into a single text string for embedding and keyword matching.
        Args:
            c: A dictionary representing a listing.
        Returns:
            A concatenated text string of the listing's key fields.
        """
        # Extract key listing fields safely
        title = c.get("title", "")
        description = c.get("description", "")
        bed_bath = f"{c.get('bedroom', '')} bedroom {c.get('bathroom', '')} bathroom"
        appliances = " ".join(c.get("amenities", {}).get("appliances", []))
        utilities = " ".join(c.get("amenities", {}).get("utilities_included", []))
        rental_terms = " ".join([str(v) for v in c.get("rental_terms", {}).values()])

        # Combine all fields into a single text blob
        return f"{title} {description} {bed_bath} {appliances} {utilities} {rental_terms}"

    def rerank(self, user_query: Dict, candidates: List[Dict]) -> List[Dict]:
        """
        Re-rank a list of candidate listings based on query semantic similarity and preferences.
        Args:
            user_query: A dictionary containing the user's query, including a "rag_content" key.
            candidates: A list of listing dictionaries to rerank.
        Returns:
            The list of candidates sorted by combined score in descending order.
        """
        # Extract and process user preferences
        rag_content = user_query.get("rag_content", "").lower()
        preferences = [p.strip() for p in rag_content.split(",") if p.strip()]

        # Encode the user query into an embedding for semantic similarity
        query_emb = self.embedder.encode(rag_content, convert_to_tensor=True)

        scores = []
        for c in candidates:
            # Build a textual representation of the listing
            text = self.build_listing_text(c)

            # Encode listing text into an embedding
            doc_emb = self.embedder.encode(text, convert_to_tensor=True)

            # 1. Embedding similarity score (cosine similarity)
            emb_score = util.cos_sim(query_emb, doc_emb).item()

            # 2. Preference keyword overlap score
            pref_score = sum(1 for pref in preferences if pref in text.lower()) / max(len(preferences), 1)

            # 3. Final hybrid score
            final_score = self.alpha * emb_score + self.beta * pref_score
            scores.append(final_score)

        # Sort candidates by score without attaching it
        sorted_candidates = [
            c for _, c in sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
        ]

        return sorted_candidates[:4]

if __name__ == "__main__":
    # Example user query
    user_query = {
        "rag_content": "pet friendly, balcony, furnished"
    }

    # Example candidate listings (suppose retrieved from MongoDB)
    candidates = [
        {"title": "2BHK Apartment", "description": "Spacious, furnished, with balcony",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 2, "bathroom": 1},
        {"title": "Studio Flat", "description": "Affordable unit",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 1, "bathroom": 1},
        {"title": "3BHK Luxury", "description": "Pet friendly furnished apartment",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 3, "bathroom": 2},
        {"title": "1BHK Cozy", "description": "Compact, balcony",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 1, "bathroom": 1},
        {"title": "4BHK Villa", "description": "Furnished, large balcony, pet friendly",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 4, "bathroom": 3},
        {"title": "Penthouse", "description": "Luxury furnished apartment with balcony",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 3, "bathroom": 2},
    ]

    reranker = SecondaryHybridReranker()

    reranked_results = reranker.rerank(user_query, candidates)

    print("Reranked Listings:")
    for r in reranked_results:
        print(f"{r['title']} - Score: {r['score']:.4f}")