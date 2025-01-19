<<<<<<< HEAD
import pandas as pd
from sklearn.metrics import roc_auc_score, confusion_matrix, roc_curve
import matplotlib.pyplot as plt
import seaborn as sns

class Evaluation:
    def __init__(self, positive_file, negative_file, score_column='score', threshold=0.5):
        if positive_file is None or negative_file is None:
            raise ValueError("Error: Both positive and negative files must be provided.")

        self.positive_file_load = pd.read_csv(positive_file)
        self.negative_file_load = pd.read_csv(negative_file)
        self.score_column = score_column

        if self.score_column not in self.positive_file_load.columns or self.score_column not in self.negative_file_load.columns:
            raise ValueError(f"Error: Score column '{score_column}' not found in the provided files.")

        self.positive_scores = self.positive_file_load[self.score_column].values
        self.negative_scores = self.negative_file_load[self.score_column].values
        self.y_true = [1] * len(self.positive_scores) + [0] * len(self.negative_scores)
        self.y_scores = list(self.positive_scores) + list(self.negative_scores)
        self.threshold = threshold

    def set_threshold(self, threshold):
        self.threshold = threshold

    def calculate_auc(self):
        auc_score = roc_auc_score(self.y_true, self.y_scores)

        return auc_score

    def show_confusion_matrix(self):
        self.y_pred = [1 if score >= self.threshold else 0 for score in self.y_scores]
        cm = confusion_matrix(self.y_true, self.y_pred)

        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=['Negative', 'Positive'], yticklabels=['Negative', 'Positive'])
        plt.xlabel('Predicted Label')
        plt.ylabel('True Label')
        plt.title('Confusion Matrix')
        plt.show()

    def plot_roc_curve(self):
        fpr, tpr, _ = roc_curve(self.y_true, self.y_scores)

        plt.figure(figsize=(10, 6))
        plt.plot(fpr, tpr, color='blue', label='ROC Curve')
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('ROC Curve')
        plt.legend(loc='best')
        plt.grid(True)
=======
import pandas as pd
from sklearn.metrics import roc_auc_score, confusion_matrix, roc_curve
import matplotlib.pyplot as plt
import seaborn as sns

class Evaluation:
    def __init__(self, positive_file, negative_file, score_column='score', threshold=0.5):
        if positive_file is None or negative_file is None:
            raise ValueError("Error: Both positive and negative files must be provided.")

        self.positive_file_load = pd.read_csv(positive_file)
        self.negative_file_load = pd.read_csv(negative_file)
        self.score_column = score_column

        if self.score_column not in self.positive_file_load.columns or self.score_column not in self.negative_file_load.columns:
            raise ValueError(f"Error: Score column '{score_column}' not found in the provided files.")

        self.positive_scores = self.positive_file_load[self.score_column].values
        self.negative_scores = self.negative_file_load[self.score_column].values
        self.y_true = [1] * len(self.positive_scores) + [0] * len(self.negative_scores)
        self.y_scores = list(self.positive_scores) + list(self.negative_scores)
        self.threshold = threshold

    def set_threshold(self, threshold):
        self.threshold = threshold

    def calculate_auc(self):
        auc_score = roc_auc_score(self.y_true, self.y_scores)

        return auc_score

    def show_confusion_matrix(self):
        self.y_pred = [1 if score >= self.threshold else 0 for score in self.y_scores]
        cm = confusion_matrix(self.y_true, self.y_pred)

        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=['Negative', 'Positive'], yticklabels=['Negative', 'Positive'])
        plt.xlabel('Predicted Label')
        plt.ylabel('True Label')
        plt.title('Confusion Matrix')
        plt.show()

    def plot_roc_curve(self):
        fpr, tpr, _ = roc_curve(self.y_true, self.y_scores)

        plt.figure(figsize=(10, 6))
        plt.plot(fpr, tpr, color='blue', label='ROC Curve')
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('ROC Curve')
        plt.legend(loc='best')
        plt.grid(True)
>>>>>>> upstream/feature-phase2
        plt.show()