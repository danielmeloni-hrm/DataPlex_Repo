import argparse
from loader import load_config, load_quality_rules
from dataplex_client import DataplexClient

def run_deploy(env_config_path):
    # 1. Carica configurazione ambiente
    config = load_config(env_config_path)
    client = DataplexClient(config['project_id'], config['location'])
    
    # 2. Carica tutte le definizioni di qualità
    all_rules = load_quality_rules("data_quality/")
    
    # 3. Itera e deploya su GCP
    for rule_file in all_rules:
        for scan_id, details in rule_file['scans'].items():
            client.create_or_update_dq_scan(scan_id, details['table'], details['rules'])
    
    print("--- DEPLOY COMPLETATO CON SUCCESSO ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Percorso al file config (es. config/prod.yml)")
    args = parser.parse_args()
    run_deploy(args.config)