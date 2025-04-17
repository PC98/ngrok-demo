from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/hello')
def register():
    return jsonify({"message": "Hello, World!"})

def create_app():
    return app
