import os
import re

def parse_best_fit_model(directory):
    """
    Parse the best-fit model from all log files in the given directory.

    Args:
        directory (str): Path to the directory containing log files.

    Returns:
        dict: A dictionary with filenames as keys and best-fit models as values.
    """
    best_fit_models = {}

    for filename in os.listdir(directory):
        if filename.endswith(".log"):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r') as file:
                content = file.read()
                match = re.search(r"Best-fit model: (\S+) chosen according to BIC", content)
                if match:
                    best_fit_models[filename[:-4]] = match.group(1)

    return best_fit_models

if __name__ == "__main__":
    directory = input("Enter the directory containing log files: ").strip()
    if os.path.isdir(directory):
        models = parse_best_fit_model(directory)
        for model_file, model in models.items():
            print(f"\"{model_file}\": \"{model}\",")
    else:
        print("Invalid directory path.")
