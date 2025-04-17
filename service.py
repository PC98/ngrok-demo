from flask import Flask, request, jsonify

app = Flask(__name__)

POD_URLS = [
    "http://54.245.174.107:3000",
    "http://44.246.33.16:3000"
]

@app.route('/register', methods=['POST'])
def register():
    return jsonify({"message": "Hello, World!"})

def create_app():
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
