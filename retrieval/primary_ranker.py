from typing import List, Dict
from sentence_transformers import SentenceTransformer, util
import logging
import yaml

class PrimaryHybridReranker:
    """For handling the listings if it is less than 5."""

    @staticmethod
    def load_params(params_path: str) -> dict:
        """Load parameters from a YAML file."""
        try:
            with open(params_path, 'r') as file:
                params = yaml.safe_load(file)
            logging.debug('Parameters retrieved from %s', params_path)
            return params
        except FileNotFoundError:
            logging.error('File not found: %s', params_path)
            raise
        except yaml.YAMLError as e:
            logging.error('YAML error: %s', e)
            raise
        except Exception as e:
            logging.error('Unexpected error: %s', e)
            raise

    CONFIG = load_params("params.yaml")["primary_ranker"]

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
                 alpha: float = CONFIG["alpha"], beta: float = CONFIG["beta"]):
        """
        Initialize the reranker.
        Args:
            model_name: HuggingFace model for embeddings
            alpha: weight for embedding similarity
            beta: weight for preference keyword overlap
        """
        self.embedder = SentenceTransformer(model_name, device="cpu")
        self.alpha = alpha
        self.beta = beta

    def build_listing_text(self, c: Dict) -> str:
        """
        Build a text representation of a MongoDB listing
        using the most relevant fields for semantic scoring.
        """
        # Extract fields safely
        title = c.get("title", "")
        description = c.get("description", "")

        # Bedrooms / bathrooms
        bed_bath = f"{c.get('bedroom', '')} bedroom {c.get('bathroom', '')} bathroom"

        # Appliances (list under amenities.appliances)
        appliances = " ".join(c.get("amenities", {}).get("appliances", []))

        # Utilities included (list under amenities.utilities_included)
        utilities = " ".join(c.get("amenities", {}).get("utilities_included", []))

        # Rental terms (dict values like rent, fee, etc.)
        rental_terms = " ".join([str(v) for v in c.get("rental_terms", {}).values()])

        # Final text blob
        text = f"{title} {description} {bed_bath} {appliances} {utilities} {rental_terms}"
        return text

    def rerank(self, user_query: Dict, candidates: List[Dict]) -> List[Dict]:
        """
        Hybrid reranker: combines embedding similarity + preference matching
        Args:
            user_query: {"location": "Greater Noida", "price": "", "rag_content": "pet friendly, balcony, furnished"}
            candidates: list of listing dicts
        Returns:
            Sorted list of candidates (without attaching scores)
        """
        rag_content = user_query.get("rag_content", "").lower()
        preferences = [p.strip() for p in rag_content.split(",") if p.strip()]

        # Encode the user's preference query into an embedding
        query_emb = self.embedder.encode(rag_content, convert_to_tensor=True)

        scores = []
        for c in candidates:
            # Build text representation of the listing
            text = self.build_listing_text(c)

            # Encode the candidate listing into an embedding
            doc_emb = self.embedder.encode(text, convert_to_tensor=True)

            # 1. Embedding similarity score
            emb_score = util.cos_sim(query_emb, doc_emb).item()

            # 2. Preference keyword overlap score
            pref_score = sum(1 for pref in preferences if pref in text.lower()) / max(len(preferences), 1)

            # 3. Hybrid score
            final_score = self.alpha * emb_score + self.beta * pref_score
            scores.append(final_score)

        # Sort candidates by score (without attaching the score)
        sorted_candidates = [c for _, c in sorted(zip(scores, candidates),
                                                key=lambda x: x[0],
                                                reverse=True)]
        return sorted_candidates


# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    user_message = {
        "location": "Greater Noida",
        "price": "",
        "rag_content": "pet friendly, balcony, furnished"
    }

    candidates = [
        {"title": "2BHK Apartment", "description": "Spacious, furnished, with balcony",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 2, "bathroom": 1},
        {"title": "Studio Flat", "description": "Affordable unit",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 1, "bathroom": 1},
        {"title": "3BHK Luxury", "description": "Pet friendly furnished apartment",
         "amenities": {"appliances": [], "utilities_included": []}, "bedroom": 3, "bathroom": 2},
    ]

    reranker = PrimaryHybridReranker()
    reranked = reranker.rerank(user_message, candidates)

    for r in reranked:
        print(r["title"], r["score"])
