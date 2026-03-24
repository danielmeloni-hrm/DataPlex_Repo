import os
import yaml
import argparse
from dataplex_client import DataplexClient
from deploy_quality import deploy_data_quality

def run_deploy(config_path):
    # 1. Carica la config base (project_id, location)
    with open(config_path, 'r') as f:
        main_config = yaml.safe_load(f)

    # 2. Scansiona la cartella data_quality per trovare i file .yml
    all_scans = []
    dq_path = "data_quality" 
    
    if os.path.exists(dq_path):
        for root, dirs, files in os.walk(dq_path):
            for file in files:
                if file.endswith(".yml") or file.endswith(".yaml"):
                    full_path = os.path.join(root, file)
                    with open(full_path, 'r') as f:
                        scan_content = yaml.safe_load(f)
                        if scan_content and "scans" in scan_content:
                            # Estraiamo le scansioni dal file
                            for scan_id, details in scan_content["scans"].items():
                                # Inseriamo i metadati necessari
                                details["id"] = scan_id
                                details["source_file"] = full_path 
                                
                                # --- MODIFICA FONDAMENTALE ---
                                # Passiamo l'intero dizionario 'details' che ora include 
                                # anche il blocco 'alerts' se presente nello YAML
                                all_scans.append(details)
    
    # 3. Aggiorna la config con la lista delle scansioni trovate
    main_config["scans"] = all_scans

    # 4. Inizializza il client Dataplex (che ora ha anche il Monitoring Client dentro)
    client = DataplexClient(main_config['project_id'], main_config['location'])
    
    # 5. Lancia il deploy passando il client
    # Nota: non serve passare alert_manager separato perché è integrato in client
    deploy_data_quality(client, main_config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    run_deploy(args.config)