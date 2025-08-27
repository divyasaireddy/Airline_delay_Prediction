 ✈️ Flight Delay Prediction

📁 Project Overview  
This project predicts flight delays using historical flight data, airline and airport details, and weather conditions. It uses Apache Spark with Scala for data processing, feature engineering, model training, and console-based output visualization.

📦 Dependencies  
Make sure you have the following installed:
- Apache Spark
- Scala
- IntelliJ IDEA with Scala plugin
- SBT (Scala Build Tool)

📊 Dataset Description  
Stored in the `data/` folder:

- `fact_flight_delays.csv`: Flight delay facts (delay minutes, categories, etc.)
- `dim_airline.csv`: Airline details
- `dim_airport.csv`: Airport details
- `dim_weather.csv`: Weather conditions

🧹 Data Loading (`DataLoader.scala`)  
- Loads all CSV files from the `data/` folder  
- Prepares DataFrames for further processing  
- Handles schema inference and header options

🛠️ Feature Engineering (`FeatureEngineering.scala`)  
- Joins fact and dimension tables  
- Creates new features for modeling  
- Prepares the final dataset for training

🧠 Model Training (`ModelTrainer.scala`)  
- Trains a rule-based classification model  
- Uses engineered features to predict delay categories  
- Saves predictions for analysis

📋 Output Visualization (`Visualizer.scala`)  
- Joins predictions with airline, airport, and weather data  
- Prints summary outputs to the console:
  - Distribution of predicted delay categories  
  - Average delay by category, airline, and city  
  - Prediction accuracy  
  - Weather impact on delays  
- **Note**: This file does not generate charts, only console outputs


🚀 How to Run in IntelliJ  
1. Open the project in IntelliJ IDEA  
2. Ensure Spark and Scala are configured  
3. Place CSV files in the `data/` folder  
4. Run the files in this order:
   - `DataLoader.scala`  
   - `FeatureEngineering.scala`  
   - `ModelTrainer.scala`  
   - `Visualizer.scala`

✅ Future Improvements  
- Add chart-based visualizations using Plotly or Vegas  
- Export outputs to CSV or HTML reports  
- Use advanced ML models like Decision Trees or Gradient Boosting  
- Build a dashboard for interactive analysis

