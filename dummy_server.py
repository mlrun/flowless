from flask import request, jsonify, Flask

app = Flask(__name__)

@app.route('/', methods=['POST'])
def create_task():
    print(request.json)
    return jsonify({'data': 99}), 200


if __name__ == '__main__':
    app.run(debug=True)