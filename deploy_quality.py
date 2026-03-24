import re
import pytz
from datetime import datetime
from google.cloud import dataplex_v1

# Costanti per il tracciamento
MANAGED_LABEL_KEY = "managed_by"
MANAGED_LABEL_VALUE = "dataplex_deployer"

def convert_cron_to_utc(cron_expression, timezone_name):
    """Converte un'espressione cron dal timezone specificato a UTC."""
    if timezone_name == "UTC" or not cron_expression:
        return cron_expression
    try:
        local_tz = pytz.timezone(timezone_name)
        parts = cron_expression.split()
        if len(parts) >= 2 and parts[1].isdigit():
            local_hour, local_minute = int(parts[1]), int(parts[0])
            now = datetime.now()
            local_time = local_tz.localize(
                datetime(now.year, now.month, now.day, local_hour, local_minute)
            )
            utc_time = local_time.astimezone(pytz.UTC)
            parts[0], parts[1] = str(utc_time.minute), str(utc_time.hour)
            return " ".join(parts)
    except Exception as e:
        print(f"⚠️ Errore conversione timezone ({timezone_name}): {e}")
    return cron_expression

def build_rule(rule_dict):
    """
    Costruisce la regola per Dataplex. 
    Nota: usiamo sql_assertion come fallback per aggirare i bug dei campi row_count.
    """
    rule = {
        "dimension": rule_dict.get("dimension", "VALIDITY").upper()
    }
    
    if rule_dict.get("column"):
        rule["column"] = rule_dict["column"]

    # 1. Gestione SQL ASSERTION (Consigliata per Viste e flessibilità)
    if "sql_assertion" in rule_dict or "sql" in rule_dict:
        sql_text = rule_dict.get("sql") or rule_dict.get("sql_assertion", {}).get("sql_statement")
        # Racchiudiamo tra parentesi per sicurezza di parsing
        if not sql_text.startswith("("):
            sql_text = f"({sql_text})"
        rule["sql_assertion"] = {"sql_statement": sql_text}
    
    # 2. Fallback automatico per row_count_expectation (trasformata in SQL)
    elif "row_count_expectation" in rule_dict:
        min_count = rule_dict["row_count_expectation"].get("min_count", 0)
        # Trasformazione in asserzione SQL per evitare errori di 'Unknown field'
        rule["sql_assertion"] = {"sql_statement": f"(COUNT(*) >= {min_count})"}
    
    # 3. Regole standard (es. COMPLETENESS / non_null)
    else:
        if rule["dimension"] == "COMPLETENESS" or "non_null_expectation" in rule_dict:
            rule["non_null_expectation"] = {}
            # Il threshold è obbligatorio per le regole standard
            rule["threshold"] = float(rule_dict.get("threshold", 1.0))

    return rule

def deploy_data_quality(deployer, config, environment=None, property_scope="all", dry_run=False):
    """Orchestra il deploy delle scansioni e degli alert associati."""
    
    env = environment or config.get("environment", "default")
    print(f"\n🚀 Avvio Deploy: env={env}, scope={property_scope}")
    
    # La config contiene una lista di scansioni pre-caricate dal loader
    scans_list = config.get('scans', [])
    print(f"📂 Numero di scansioni trovate nella config: {len(scans_list)}")

    for scan_cfg in scans_list:
        source_file = scan_cfg.get("source_file", "N/A")
        scan_id = scan_cfg.get("id", "Unknown_ID")
        
        print(f"\n--- 📁 Lettura file: {source_file} ---")
        print(f"🔎 Elaborazione Scan ID: {scan_id}")

        # 1. Filtro Scope (es. per gestire solo sottocartelle specifiche)
        prop_key = source_file.split("/")[0] if "/" in source_file else "default"
        if property_scope != "all" and prop_key != property_scope:
            print(f"⏭️  Salto scansione (fuori scope: {prop_key})")
            continue

        # 2. Preparazione Regole
        rules_processed = [build_rule(r) for r in scan_cfg.get("rules", [])]
        
        # 3. Gestione Cron e Timezone
        cron_expression = scan_cfg.get("schedule", "0 * * * *")
        timezone = scan_cfg.get("timezone", "UTC")
        utc_cron = convert_cron_to_utc(cron_expression, timezone)

        # 4. Configurazione Alert
        alert_cfg = scan_cfg.get("alerts")
        has_alerts = alert_cfg and alert_cfg.get("enabled", False)

        # 5. Esecuzione Deploy
        resource_path = scan_cfg.get("resource_path")
        row_filter = scan_cfg.get("row_filter")

        if not resource_path:
            print(f"❌ Errore: 'resource_path' mancante per {scan_id}")
            continue

        print(f"📍 Target Resource: {resource_path}")
        if has_alerts:
            print(f"🔔 Alerting abilitato: {len(alert_cfg.get('notification_channels', []))} canali configurati.")

        if dry_run:
            print(f"🧪 [DRY-RUN] Upsert scan: {scan_id}")
            print(f"   Schedule UTC: {utc_cron}")
            print(f"   Rules: {len(rules_processed)}")
            if row_filter: print(f"   Filter: {row_filter}")
        else:
            try:
                # Chiamata al metodo nel dataplex_client.py
                deployer.create_or_update_dq_scan(
                    scan_id=scan_id, 
                    table_path=resource_path, 
                    rules=rules_processed,
                    schedule_cron=utc_cron,
                    row_filter=row_filter,
                    alert_config=alert_cfg # Passiamo l'intero blocco alert
                )
            except Exception as e:
                print(f"💥 Errore critico durante il deploy di {scan_id}: {e}")

    print("\n✅ Operazione conclusa.")