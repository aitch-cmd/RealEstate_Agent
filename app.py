import streamlit as st
from pipeline import RentalPipeline
from rental_agent import RentalAgent

# Initialize pipeline + chatbot once
if "pipeline" not in st.session_state:
    st.session_state.pipeline = RentalPipeline()
if "agent" not in st.session_state:
    st.session_state.agent = RentalAgent()

st.title("🏠 Rental Chatbot")

# Input box
user_input = st.text_input("Ask about rentals:",)

# Process on button click
if st.button("Send") and user_input:
    with st.spinner("Searching the best listings..."):
        # Step 1: pipeline produces final listings
        final_listings = st.session_state.pipeline.process_user_message(user_input)

        # Step 2: agent generates natural response
        response = st.session_state.agent.generate_response(user_input, final_listings)

    # Show results
    st.subheader("Bot Response")
    st.write(response)

    # Optionally preview raw listings
    with st.expander("See retrieved listings"):
        st.json(final_listings)
