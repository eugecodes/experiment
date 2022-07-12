from flask import Flask, render_template
import json

app = Flask(__name__)

@app.route("/")
def main():
    # Let's get the json data
    data = []
    f = open('coins.json')
    data = json.load(f)
    #p = open('bitco.json')
    #data1 = json.load(f)
    #z = open('bitco_shifted.json')
    #data2 = json.load(f)
    return render_template('main.html', data=data)