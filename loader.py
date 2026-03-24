import yaml
import os

def load_config(file_path):
    with open(file_path, 'r') as stream:
        return yaml.safe_load(stream)

def load_quality_rules(directory):
    # Cerca tutti i file .yml nelle sottocartelle di data_quality/
    rules = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".yml"):
                rules.append(load_config(os.path.join(root, file)))
    return rules