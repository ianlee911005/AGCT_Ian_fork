import pandas as pd
import matplotlib.pyplot as plt
import os

class DataLoader:
    def __init__(self, task=None, Name=None):
        self.Name = Name
        self.task = task
        if not self.task:
            raise ValueError("Error: task type must be provided.")
        if not self.Name:
            raise ValueError("Error: data name must be provided.")
        module_dir = os.path.dirname(__file__)
        self.file_path = str(module_dir).replace('\\','/').replace('utils','datas') + '/' + self.task + '/' + self.Name + '.csv'
        self.data = None

        self._load_data()

    def _load_data(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Error: The file '{self.Name}' does not exist.")

        try:
            self.data = pd.read_csv(self.file_path, dtype={'chr': str})
            print(f"Data {self.Name} loaded successfully")
        except Exception as e:
            print(f"Error loading data: {e}")

    def show_data(self, num_lines=5):
        if self.data is not None:
            print(self.data.head(num_lines))
        else:
            print("No data loaded. Please load a CSV file first.")

    def show_chromosome_distribution(self):
        if self.data is not None:
            chromosome_order = [str(i) for i in range(1, 23)] + ['X', 'Y', 'MT']
            chromosome_counts = self.data['chr'].value_counts()
            chromosome_counts = chromosome_counts.reindex(chromosome_order, fill_value=0)

            print("Chromosome Distribution:")
            print(chromosome_counts.to_frame(name='Counts'))

            chromosome_counts.plot(kind='bar')
            plt.title('Chromosome Distribution')
            plt.xlabel('Chromosome')
            plt.ylabel('Count')
            plt.show()
        else:
            print("No data loaded. Please load a CSV file first.")

    def show_gene_distribution(self, top_n = None):
        if self.data is not None:
          gene_counts = self.data['genename'].value_counts()

          with pd.option_context('display.max_rows', None):
            if top_n is None:
                top_genes = gene_counts
                print("Gene Distribution:")
            else:
                top_genes = gene_counts.head(top_n)
                print(f"Top {top_n} Gene Distribution:")

            print(top_genes.to_frame(name='Counts'))

        else:
            print("No data loaded. Please load a CSV file first.")

    def filter_data(self, column, value):
        if self.data is not None:
            filtered_data = self.data[self.data[column] == value]
            print(f"Filtered data by {column} = {value}:")
            return filtered_data
        else:
            print("No data loaded. Please load a CSV file first.")
            return None

    def output_data(self, output_format='csv', output_file='output'):
        if self.data is not None:
            columns_to_output = ['chr', 'pos', 'ref', 'alt']
            output_data = self.data[columns_to_output]

            if output_format == 'csv':
                output_data.to_csv(f"{output_file}.csv", index=False)
                print(f"Data successfully saved to {output_file}.csv")
            elif output_format == 'txt':
                output_data.to_csv(f"{output_file}.txt", index=False, sep='\t')
                print(f"Data successfully saved to {output_file}.txt")
            else:
                print("Unsupported format. Please choose 'csv' or 'txt'.")
        else:
            print("No data loaded. Please load a CSV file first.")
            
    def exclude_training_data(self, training_file):
        if not os.path.exists(training_file):
            raise FileNotFoundError(f"Error: The training file '{training_file}' does not exist.")
        
        try:
            training_data = pd.read_csv(training_file)
            print(f"Training data loaded successfully from {training_file}")

            key_columns = ['chr', 'pos', 'ref', 'alt']
            for col in key_columns:
                if col not in training_data.columns:
                    raise ValueError(f"Error: Column '{col}' must be present in both datasets.")

            before_exclusion = len(self.data)
            self.data = pd.merge(self.data, training_data, on=key_columns, how='left', indicator=True)
            self.data = self.data[self.data['_merge'] == 'left_only'].drop(columns=['_merge'])
            after_exclusion = len(self.data)
            
            print(f"Exclusion complete. {before_exclusion - after_exclusion} rows excluded.")
            print(f"Remaining data points: {after_exclusion}")

        except Exception as e:
            print(f"Error processing training data: {e}")