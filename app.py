from flask import Flask, request, render_template
from pipeline import RentalPipeline
from rental_agent import RentalAgent

# Initialize pipeline and agent
pipeline = RentalPipeline()
agent = RentalAgent()

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/search", methods=["POST"])
def search():
    data = request.get_json()
    if not data or "query" not in data:
        return "Error: No query provided.", 400

    user_input = data["query"]
    final_listings = pipeline.process_user_message(user_input)
    response = agent.generate_response(user_input, final_listings)

    return str(response)

if __name__ == "__main__":
    app.run(debug=True)
