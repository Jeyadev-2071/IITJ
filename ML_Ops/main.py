import optuna
import joblib
from flask import Flask, request, render_template, redirect, url_for
import pandas as pd
import numpy as np

model = joblib.load('model/best_model_random_forest.pkl')

columns = ['CRIM', 'ZN', 'INDUS', 'CHAS', 'NOX', 'RM', 'AGE', 'DIS', 'RAD', 'TAX', 'PT', 'B', 'LSTAT']

# Create Flask app
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
def predict():
    try:
        input_format = request.form.get("inputFormat")
        if input_format == "json":
            
            json_data = request.form["jsonData"]
            input_data = pd.read_json(json_data)
        else:
            input_data = [
                float(request.form[column]) for column in columns
            ]
            input_data = pd.DataFrame([input_data], columns=columns)
        
     
        predictions = model.predict(input_data)
        prediction_texts = [f'Predicted Median Value of Owner-Occupied Homes: ${pred:.2f}' for pred in predictions]
        return render_template('result.html', prediction_text=prediction_texts)
    except Exception as e:
        return render_template('result.html', prediction_text=[f'Error occurred: {str(e)}'])

@app.route('/result')
def prediction_result():
    prediction_text = request.args.get('prediction_text')
    return render_template('result.html', prediction_text=prediction_text)

if __name__ == '__main__':
    app.run(debug=True)