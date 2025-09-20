from langchain.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from retrieval.mgdb_filter import MongoDBFilter
from langchain.memory import ConversationBufferMemory
import os
from dotenv import load_dotenv

# Load API key
load_dotenv()
google_api_key = os.getenv("GOOGLE_API_KEY")

class RentalAgent:
    def __init__(self):
        # Initialize LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-pro",
            temperature=0.5,
            api_key=google_api_key
        )

        # MongoDB filter
        self.executor = MongoDBFilter()

        # Memory to store conversation
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )

        # Prompt template
        self.prompt_template = ChatPromptTemplate.from_template("""
        You are a helpful real estate assistant.
        The user asked: {user_message}

        Conversation history:
        {chat_history}

        Here are the listings retrieved from MongoDB:
        {listings}

        Format them into a natural, friendly response highlighting:
        - Address
        - Rent Price
        - Bedrooms & Bathrooms
        - Availability date (if present)
        - Any other descriptive detail other than mentioned above in a concise format.

        If a user asks details about a specific listing, provide the details if present in the listing.  
        **Special Note**: Make sure to list the listings in the order they were provided.                                                 
                                                                
        Give the answer in proper format with bullet points for each listing.
        If no listings are found, politely explain that and suggest trying another query.
        """)

    def generate_response(self, user_input: str, reranked_listings: list) -> str:
        """Generate chatbot response with memory"""

        # Step 1: Use provided reranked listings instead of searching again
        listings_text = str(reranked_listings) if reranked_listings else "[]"

        # Step 2: Add user input to memory
        self.memory.chat_memory.add_user_message(user_input)

        # Step 3: Format prompt
        prompt = self.prompt_template.format_messages(
            user_message=user_input,
            chat_history=self.memory.load_memory_variables({})["chat_history"],
            listings=listings_text
        )

        # Step 4: Generate response
        response = self.llm.invoke(prompt)
        bot_reply = response.content

        # Step 5: Save assistant response to memory
        self.memory.chat_memory.add_ai_message(bot_reply)

        return bot_reply

# Example usage
if __name__ == "__main__":
    chatbot = RentalChatbot()
    while True:
        user_input = input("You: ")
        if user_input.lower() in ["exit", "quit"]:
            break
        response = chatbot.generate_response(user_input)
        print("Bot:", response)
