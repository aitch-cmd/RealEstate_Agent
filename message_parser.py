import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import StructuredOutputParser, ResponseSchema


class UserMessageParser:
    def __init__(self, model_name: str = "gemini-2.5-pro", temperature: float = 0.7):
        # Load environment variables
        load_dotenv()
        google_api_key = os.environ.get("GOOGLE_API_KEY")

        # Define schema
        response_schemas = [
            ResponseSchema(name="location", description="The city or place mentioned by the user"),
            ResponseSchema(name="price", description="The budget or price mentioned by the user in numeric form"),
            ResponseSchema(
                name="rag_content",
                description="Extra context or requirements like number of bedrooms, furnished, etc."
            )
        ]

        # Parser + format instructions
        self.output_parser = StructuredOutputParser.from_response_schemas(response_schemas)
        self.format_instructions = self.output_parser.get_format_instructions()

        # LLM initialization
        self.llm = ChatGoogleGenerativeAI(
            model=model_name,
            temperature=temperature,
            google_api_key=google_api_key
        )

        # Prompt
        self.prompt = ChatPromptTemplate.from_template("""
        You are an information extractor.
        Extract structured details from the user message.

        User message: {user_message}

        {format_instructions}
        """)

        # Chain
        self.chain = self.prompt | self.llm | self.output_parser

    def extract(self, user_message: str):
        """Extract structured info (location, price, rag_content) from user message"""
        return self.chain.invoke({
            "user_message": user_message,
            "format_instructions": self.format_instructions
        })

if __name__ == "__main__":
    extractor = UserMessageExtractor()
    msg = "Show me 2BHK flats in Bangalore under 20k"
    result = extractor.extract(msg)
    print(result)
