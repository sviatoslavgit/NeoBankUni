from flask import Flask, request, jsonify
import joblib
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

app = Flask(__name__)

# Load your model and scaler
model = joblib.load('logistic_regression_model.pkl')
scaler = MinMaxScaler(feature_range=(-120, 120))

# Preprocess incoming data
def preprocess_incoming_data(data):
    # Drop the 'Time' column if it's not needed for prediction
    if 'Time' in data.columns:
        data = data.drop(columns=['Time'])

    # Ensure that all columns needed for scaling are numerical
    columns_to_scale = ['Amount', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 
                        'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 'V19', 'V20', 
                        'V21', 'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28']
    
    data[columns_to_scale] = scaler.fit_transform(data[columns_to_scale])
    return data


# Cross-validate endpoint - Save all columns to CSV including predicted values
@app.route('/cross_validate', methods=['POST'])
def cross_validate():
    try:
        # Parse incoming JSON data
        data = request.get_json()
        df = pd.DataFrame([data])
        
        # Preprocess the data and make predictions
        processed_data = preprocess_incoming_data(df)
        predictions = model.predict(processed_data)

        # Add predictions to the DataFrame
        df['Predicted_Class'] = predictions

        # Save the entire dataframe (including all features and predictions) to a CSV file
        output_filename = 'cross_validate_results.csv'
        df.to_csv(output_filename, mode='a', header=False, index=False)

        return jsonify({"message": "Data saved to CSV successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Data endpoint
@app.route('/data', methods=['POST'])
def receive_data():
    try:
        data = request.get_json()
        df = pd.DataFrame([data])
        processed_data = preprocess_incoming_data(df)
        predictions = model.predict(processed_data)
        df['Predicted_Class'] = predictions

        return jsonify(df.to_dict(orient='records')), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
