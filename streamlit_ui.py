import streamlit as st
from langchain.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from retrieval.mgdb_filter import MongoDBFilter  
import os
from dotenv import load_dotenv

# Load API key
load_dotenv()
google_api_key = os.getenv("GOOGLE_API_KEY")

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-pro",
    temperature=0.5,
    api_key=google_api_key
)

# System prompt to turn listings (dicts) into natural language
prompt_template = ChatPromptTemplate.from_template("""
You are a helpful real estate assistant.
The user asked: {user_message}

Here are the listings retrieved from MongoDB:
{listings}

Format them into a natural, friendly response highlighting:
- Address
- Rent Price
- Bedrooms & Bathrooms
- Availability date (if present)
- Any other descriptive detail other then mentioned above in a concise format.

If no listings are found, politely explain that and suggest trying another query.
""")

executor = MongoDBFilter()


def run_chatbot():
    st.set_page_config(page_title="Rental Finder Chatbot", page_icon="🏠")
    st.title("🏠 Rental Finder Chatbot")

    # Chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # User input
    user_input = st.chat_input("Type your rental query...")
    if user_input:
        # Display user input
        st.chat_message("user").markdown(user_input)
        st.session_state.messages.append({"role": "user", "content": user_input})

        # Step 1: Query rentals
        listings = executor.search_rentals(user_input)

        # Step 2: Format response with LLM
        listings_text = str(listings) if listings else "[]"
        prompt = prompt_template.format_messages(
            user_message=user_input,
            listings=listings_text
        )
        response = llm.invoke(prompt)

        # Step 3: Show response
        bot_reply = response.content
        with st.chat_message("assistant"):
            st.markdown(bot_reply)
        st.session_state.messages.append({"role": "assistant", "content": bot_reply})


if __name__ == "__main__":
    run_chatbot()
