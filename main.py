from flask import Flask, request, jsonify
from scrap import Crls

app = Flask(__name__)

@app.route('/api/indicator/<country>')
def indicator_endpoint(country):
    crls = Crls(f'https://tradingeconomics.com/{country}/indicators')
    resp = crls.indicator()
    return jsonify(resp)
    
@app.route('/api/cot', methods=['GET'])
def cot_endpoint():
    url = request.args.get('url')
    parent = request.args.get('parent')
    child = request.args.get('child')
    selector = request.args.get('selector')

    if not all([url, parent, child, selector]):
        return jsonify({'error': 'Missing required parameters: url, parent, child, selector'}), 400

    crls = Crls(url)
    headers, data = crls.cot(parent, child, selector)

    if headers and data:
        json_data = crls.to_json(headers, data)
        return jsonify(json_data)
    else:
        return jsonify({"message": "Failed to retrieve data."})

if __name__ == '__main__':
    app.run(debug=True)