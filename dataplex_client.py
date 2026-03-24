import json
from google.cloud import dataplex_v1
from google.cloud import monitoring_v3
from google.cloud.dataplex_v1.types import DataScan
from google.protobuf import field_mask_pb2
from google.api_core import client_options
class DataplexClient:
    def __init__(self, project_id, location):
        self.project = project_id # Deve essere 'analystack'
        self.location = location
        self.client = dataplex_v1.DataScanServiceClient()
        
        # RISOLUZIONE ERRORE 403: Forziamo il progetto di quota su analystack
        # Questo dice a Google: "Usa i permessi e le API di analystack, non HRM"
        options = client_options.ClientOptions(quota_project_id=project_id)
        
        self.alert_client = monitoring_v3.AlertPolicyServiceClient(
            client_options=options # <--- Passiamo le opzioni qui
        )

    def _upsert_alert_policy(self, scan_id, alert_cfg):
        # Usiamo esplicitamente self.project (che è analystack) 
        # invece di lasciare che Google decida in base al Service Account
        project_path = f"projects/{self.project}"

    def create_or_update_dq_scan(self, scan_id, table_path, rules, schedule_cron=None, row_filter=None, alert_config=None):
        clean_scan_id = scan_id.replace("_", "-").lower()
        parent = f"projects/{self.project}/locations/{self.location}"
        
        # Formattazione URI per BigQuery (fondamentale per le Viste)
        normalized_path = table_path.replace(':', '/')
        full_resource = f"//bigquery.googleapis.com/{normalized_path}"

        # 1. COSTRUIAMO IL DIZIONARIO PER DATAPLEX
        scan_dict = {
            "display_name": scan_id.replace("_", " ").title(),
            "data": {"resource": full_resource},
            "data_quality_spec": {
                "rules": rules,
                "catalog_publishing_enabled": True
            },
            "execution_spec": {
                "trigger": {"schedule": {"cron": schedule_cron}} if schedule_cron else {"on_demand": {}}
            }
        }

        if row_filter:
            scan_dict["data_quality_spec"]["row_filter"] = row_filter

        try:
            print(f"🚀 [DATAPLEX] Upserting scan: {clean_scan_id}")
            
            # Il trucco del JSON bypassa i limiti di versione della libreria
            scan_obj = DataScan.from_json(json.dumps(scan_dict))

            try:
                # Prova creazione scansione
                operation = self.client.create_data_scan(
                    parent=parent,
                    data_scan=scan_obj,
                    data_scan_id=clean_scan_id
                )
                operation.result()
                print(f"✅ SUCCESS: Scansione '{clean_scan_id}' creata!")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"🔄 Aggiornamento scansione esistente...")
                    mask = field_mask_pb2.FieldMask(paths=[
                        "data_quality_spec.rules", 
                        "data_quality_spec.row_filter", 
                        "execution_spec.trigger"
                    ])
                    scan_obj.name = f"{parent}/dataScans/{clean_scan_id}"
                    self.client.update_data_scan(data_scan=scan_obj, update_mask=mask)
                    print(f"✅ SUCCESS: Scansione aggiornata.")
                else:
                    raise e

            # 2. GESTIONE ALERT (Se presente nella configurazione)
            if alert_config and alert_config.get("enabled"):
                self._upsert_alert_policy(clean_scan_id, alert_config)

        except Exception as e:
            print(f"❌ Errore critico: {e}")
            raise e

    def _upsert_alert_policy(self, scan_id, alert_cfg):
        """Crea o aggiorna una Alert Policy basata sui log di fallimento della scansione."""
        project_path = f"projects/{self.project}"
        
        # Recuperiamo gli ID dei canali dallo YAML (coll_ids)
        # Formato atteso: projects/PROJECT_ID/notificationChannels/ID
        notification_channels = []
        for channel_group in alert_cfg.get("notification_channels", []):
            for channel_id in channel_group.get("coll_ids", []):
                notification_channels.append(f"{project_path}/notificationChannels/{channel_id}")

        # Filtro per intercettare il fallimento della specifica scansione nei log
        log_filter = (
            f'resource.type="dataplex.googleapis.com/DataScan" '
            f'resource.labels.datascan_id="{scan_id}" '
            f'jsonPayload.dataQuality.dimensionPassed.VALIDITY="false"'
        )

        alert_policy = {
            "display_name": f"Dataplex Alert: {scan_id}",
            "documentation": {
                "content": alert_cfg.get("documentation", "Fallimento qualità dati."),
                "mime_type": "text/markdown"
            },
            "conditions": [{
                "display_name": f"Fallimento DQ: {scan_id}",
                "condition_matched_log": {"filter": log_filter}
            }],
            "notification_channels": notification_channels,
            "combiner": monitoring_v3.AlertPolicy.ConditionCombinerType.OR,
            "enabled": True,
            
            # --- AGGIUNTA FONDAMENTALE PER RISOLVERE L'ERRORE 400 ---
            "alert_strategy": {
                "notification_rate_limit": {
                    "period": {"seconds": 3600} # Limita a 1 notifica ogni ora (3600 sec)
                }
            }
        }

        try:
            # Nota: Per semplicità facciamo una create. 
            # Per un vero upsert bisognerebbe prima cercare se esiste una policy con lo stesso display_name.
            self.alert_client.create_alert_policy(name=project_path, alert_policy=alert_policy)
            print(f"🔔 ALERT: Policy creata/collegata per {scan_id}")
        except Exception as e:
            if "already exists" in str(e).lower() or "409" in str(e):
                print(f"🔔 ALERT: Policy già esistente (manuale o precedente).")
            else:
                print(f"⚠️ Errore durante la configurazione Alert: {e}")