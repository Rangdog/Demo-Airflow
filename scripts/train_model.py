import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib
import glob
import os
import datetime

# Lấy thời gian chạy hiện tại
now = datetime.datetime.utcnow()
timestamp = now.strftime('%Y%m%d_%H%M%S')

# Load dữ liệu
files = glob.glob('train_results/*.csv')
dfs = [pd.read_csv(f) for f in files]
df = pd.concat(dfs)
df = df.dropna()

feature_cols = ['close', 'sma5', 'sma10', 'ema10', 'rsi14', 'macd', 'macd_signal', 'bb_high', 'bb_low']
X = df[feature_cols]
y = df['signal']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
acc = accuracy_score(y_test, y_pred)
report = classification_report(y_test, y_pred)

# Lưu model
os.makedirs('models', exist_ok=True)
model_path = f'models/model_{timestamp}.joblib'
joblib.dump(model, model_path)

# Lưu report
os.makedirs('train_reports', exist_ok=True)
report_path = f'train_reports/report_{timestamp}.txt'
with open(report_path, 'w') as f:
    f.write(f"Timestamp: {timestamp}\n")
    f.write(f"Accuracy: {acc}\n")
    f.write(report)
    f.write('\nModel path: ' + model_path)

print(f"Model saved to {model_path}")
print(f"Report saved to {report_path}")
