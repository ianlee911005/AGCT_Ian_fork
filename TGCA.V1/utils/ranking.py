import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import roc_auc_score, roc_curve
from .evaluation import Evaluation
import os

class Ranking:
    def __init__(self, positive_file_name = None, negative_file_name = None, task=None,  evaluation = 'None'):
        module_dir = os.path.dirname(__file__)
        self.task = task
        self.positive_file_path = str(module_dir).replace('\\','/').replace('utils','datas') + '/' + self.task + '/' + positive_file_name + '.csv'
        self.negative_file_path = str(module_dir).replace('\\','/').replace('utils','datas') + '/' + self.task + '/' + negative_file_name + '.csv'

        if not os.path.exists(self.positive_file_path):
            raise FileNotFoundError(f"Error: The file '{positive_file_name}' does not exist.")
        if not os.path.exists(self.negative_file_path):
            raise FileNotFoundError(f"Error: The file '{negative_file_name}' does not exist.")

        self.positive_file = pd.read_csv(self.positive_file_path)
        self.negative_file = pd.read_csv(self.negative_file_path)

        if evaluation == 'None':
            self.evaluation = None
        else:
          if not isinstance(evaluation, Evaluation):
            raise TypeError("evaluation_function must be an instance of EvaluationFunction")
          else:
            self.evaluation = evaluation

        self.model_columns = ['REVEL_score', 'gMVP_score', 'VARITY_R_LOO_score', 'ESM1b_score', 'AlphaMissense_score']

    def report_and_clean_data(self, data, dataset_name):
        for col in self.model_columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')
        nan_counts = data[self.model_columns].isna().sum()

        print(f'NaN Counts in {dataset_name} Dataset:')
        for model, count in nan_counts.items():
            print(f"  {model}: {count} NaN values ; "+ str(len(data)-int(count)) + 'remains')

    def rank_models(self):
        auc_scores = {}

        if self.evaluation is not None:
            key_columns = ['chr', 'pos', 'ref', 'alt']
            print('processing postive file...')
            for col in key_columns:
                if col not in self.evaluation.positive_file_load.columns:
                    raise ValueError(f"Error: Column '{col}' must be present in datasets.")
            
            before_exclusion = len(self.positive_file)
            self.positive_file = pd.merge(self.positive_file, self.evaluation.positive_file_load, on=key_columns, how='left', indicator=True)
            self.positive_file = self.positive_file[self.positive_file['_merge'] == 'both'].drop(columns=['_merge'])
            after_exclusion = len(self.positive_file)

            print(f"Exclusion complete. {before_exclusion - after_exclusion} rows excluded.")
            print(f"Remaining data points: {after_exclusion}")

            print('processing negative file...')
            for col in key_columns:
                if col not in self.evaluation.negative_file_load.columns:
                    raise ValueError(f"Error: Column '{col}' must be present in datasets.")
            before_exclusion = len(self.negative_file)
            self.negative_file = pd.merge(self.negative_file, self.evaluation.negative_file_load, on=key_columns, how='left', indicator=True)
            self.negative_file = self.negative_file[self.negative_file['_merge'] == 'both'].drop(columns=['_merge'])
            after_exclusion = len(self.negative_file)
            print(f"Exclusion complete. {before_exclusion - after_exclusion} rows excluded.")
            print(f"Remaining data points: {after_exclusion}")

        self.report_and_clean_data(self.positive_file, 'Positive')
        self.report_and_clean_data(self.negative_file, 'Negative')
           
        for model in self.model_columns:
            positive_scores = pd.to_numeric(self.positive_file[model], errors='coerce').dropna().values
            negative_scores = pd.to_numeric(self.negative_file[model], errors='coerce').dropna().values

            y_true = [1] * len(positive_scores) + [0] * len(negative_scores)
            y_scores = list(positive_scores) + list(negative_scores)

            if model == 'ESM1b_score':
              y_scores = [-score for score in y_scores]

            auc_score = roc_auc_score(y_true, y_scores)
            auc_scores[model] = auc_score

            fpr, tpr, _ = roc_curve(y_true, y_scores)
            plt.plot(fpr, tpr, label=f'{model} (AUC = {auc_score:.4f})')

        if self.evaluation is not None:
          fpr, tpr, _ = roc_curve(self.evaluation.y_true, self.evaluation.y_scores)
          plt.plot(fpr, tpr, label='your model (AUC = ' + str(self.evaluation.calculate_auc()) + ')')
          auc_scores['your model'] = self.evaluation.calculate_auc()

        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('ROC Curve for All Models')
        plt.legend(loc='best')
        plt.grid(True)
        plt.show()

        ranked_models = sorted(auc_scores.items(), key=lambda x: x[1], reverse=True)

        print("Model Ranking based on AUC Scores:")
        for i, (model, score) in enumerate(ranked_models, start=1):
            print(f"{i}. {model} - AUC Score: {score:.4f}")

        return