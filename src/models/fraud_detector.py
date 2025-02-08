# src/models/fraud_detector.py
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib

class FraudDetector:
    def __init__(self, random_state=42):
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=random_state
        )
        
    def train(self, features, labels):
        """Train the fraud detection model"""
        self.model.fit(features, labels)
        
        # Get training predictions
        y_pred = self.model.predict(features)
        print("\nModel Performance:")
        print(classification_report(labels, y_pred))
        
        return self
    
    def predict(self, features):
        """Make predictions on new data"""
        return self.model.predict_proba(features)
    
    def save_model(self, filepath):
        """Save model to disk"""
        joblib.dump(self.model, filepath)
    
    @classmethod
    def load_model(cls, filepath):
        """Load model from disk"""
        instance = cls()
        instance.model = joblib.load(filepath)
        return instance