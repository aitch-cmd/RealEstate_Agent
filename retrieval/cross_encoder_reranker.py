from sentence_transformers import CrossEncoder
from typing import List, Dict
from retrieval.primary_ranker import PrimaryHybridReranker

class CrossEncoderReranker:
    """
    Cross-Encoder Reranker class for listings (used when candidates are <= 4).
    Uses a pretrained cross-encoder to compute query-document relevance directly.
    """

    def __init__(self, model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"):
        """
        Initialize the cross-encoder reranker.
        Args:
            model_name: Pretrained CrossEncoder model.
        """
        self.model = CrossEncoder(model_name, device="cpu")

    def build_listing_text(self, c: Dict) -> str:
        """Convert a listing dictionary into a single text string."""
        title = c.get("title", "")
        description = c.get("description", "")
        bed_bath = f"{c.get('bedroom', '')} bedroom {c.get('bathroom', '')} bathroom"
        appliances = " ".join(c.get("amenities", {}).get("appliances", []))
        utilities = " ".join(c.get("amenities", {}).get("utilities_included", []))
        rental_terms = " ".join([str(v) for v in c.get("rental_terms", {}).values()])

        return f"{title} {description} {bed_bath} {appliances} {utilities} {rental_terms}"

    def rerank(self, user_query: Dict, candidates: List[Dict]) -> List[Dict]:
        """
        Re-rank candidates using a cross-encoder.
        Args:
            user_query: Dict with "rag_content" key containing query text.
            candidates: List of candidate listings.
        Returns:
            List of candidates sorted by cross-encoder score.
        """
        rag_content = user_query.get("rag_content", "").strip()
        if not rag_content or not candidates:
            return candidates  # no query or no candidates

        # Prepare input pairs (query, document)
        pairs = [(rag_content, self.build_listing_text(c)) for c in candidates]

        # Get cross-encoder scores
        scores = self.model.predict(pairs)

        # Sort candidates by score without attaching scores
        sorted_candidates = [c for _, c in sorted(zip(scores, candidates), 
                                                 key=lambda x: x[0], 
                                                 reverse=True)]

        return sorted_candidates[:4]
