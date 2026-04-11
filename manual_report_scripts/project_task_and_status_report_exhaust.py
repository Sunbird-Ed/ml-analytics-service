import json
import csv
import sys
import requests
import argparse
from datetime import datetime

def execute_report(report_name, report_config, expected_headers, out_prefix, druid_url, pairs, dedup_uuid, additional_filters=None):
    print(f"\n--- Generating {report_name} ---")
    query_url = f"{druid_url}/druid/v2/?pretty"
    
    metrics = report_config.get("metrics", [])
    if not metrics or "druidQuery" not in metrics[0]:
        print(f"Skipping {report_name}: Missing metrics[0].druidQuery in config")
        return
        
    base_query = metrics[0]["druidQuery"]
    labels_map = report_config.get("labels", {})
    output_metrics = report_config.get("output", [{}])[0].get("metrics", [])
    
    druid_columns = [m for m in output_metrics]
    
    for pair in pairs:
        program_id = pair.get("program_id")
        solution_id = pair.get("solution_id")
        if not program_id or not solution_id:
            continue
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_file = f"{out_prefix}_{solution_id}_{timestamp}.csv"
        
        with open(out_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=expected_headers)
            writer.writeheader()

            seen_uuids = set()
                
            payload = json.loads(json.dumps(base_query))
            if "filter" not in payload:
                payload["filter"] = {"type": "and", "fields": []}
            elif payload["filter"]["type"] != "and":
                payload["filter"] = {"type": "and", "fields": [payload["filter"]]}
                
            payload["filter"]["fields"].extend([
                {"type": "selector", "dimension": "program_id", "value": program_id},
                {"type": "selector", "dimension": "solution_id", "value": solution_id}
            ])
            if additional_filters:
                payload["filter"]["fields"].extend(additional_filters)
            
            print(f"Querying {report_name} for program_id={program_id}, solution_id={solution_id} ...")
            try:
                response = requests.post(query_url, json=payload)
                response.raise_for_status()
            except Exception as e:
                print(f"Error executing Druid query: {e}")
                continue
            
            results = response.json()
            if not results:
                continue
                
            for res in results:
                events = res.get("events", [])
                for event in events:
                    row = {}
                    actual_event = event.get("event") if isinstance(event, dict) and "event" in event else event
                    
                    if dedup_uuid:
                        uuid = str(actual_event.get("createdBy", "")).strip()
                        if uuid in seen_uuids:
                            continue
                        seen_uuids.add(uuid)
                    
                    for d_col in druid_columns:
                        header = labels_map.get(d_col, d_col)
                        val = actual_event.get(d_col, "")
                        if isinstance(val, list):
                            val = ", ".join([str(v) for v in val])
                        
                        if header in expected_headers:
                            row[header] = val
                            
                    writer.writerow(row)
                    
        print(f"Successfully generated {out_file}")

def main():
    parser = argparse.ArgumentParser(description="Merged project task and status report using Druid")
    parser.add_argument("--config", default="config.ini", help="Path to JSON config file")
    parser.add_argument("--out_task_prefix", default="task_report", help="Prefix for output Task CSV")
    parser.add_argument("--out_status_prefix", default="status_report", help="Prefix for output Status CSV")
    
    args = parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error loading config {args.config}: {e}")
        sys.exit(1)

    druid_url = config.get("druid_url", "http://localhost:8082")
    pairs = config.get("pairs", [])
    if not pairs:
        print("No program/solution pairs provided in config.")
        return

    task_config = config.get("ml-task-detail-exhaust")
    if task_config:
        # Expected Headers constructed securely from the explicit map in the task JSON
        expected_headers_task = [
            "UUID", "User Type", "User sub type", "Declared State", "District", 
            "Block", "School Name", "School ID", "Declared Board", "Org Name", 
            "Program Name", "Program ID", "Project ID", "Project Title", "Project Objective", 
            "Category", "Project start date of the user", "Project completion date of the user", 
            "Project Duration", "Project Status", 
            "Tasks", "Sub-Tasks", "Task Evidence", "Task Remarks", "Project Evidence", "Project Remarks"
        ]
        execute_report("Task Detail Exhaust", task_config, expected_headers_task, args.out_task_prefix, druid_url, pairs, dedup_uuid=False, additional_filters=[{"type": "selector", "dimension": "status_of_project", "value": "submitted"}])

    status_config = config.get("ml-project-status-exhaust")
    if status_config:
        # Expected Headers constructed securely from the explicit map in the status JSON
        expected_headers_status = [
            "UUID", "User Type", "User sub type", "Declared State", "District", 
            "Block", "School Name", "School ID", "Declared Board", "Org Name", 
            "Program Name", "Program ID", "Project ID", "Project Title", "Project Objective", 
            "Project start date of the user", "Project completion date of the user", 
            "Project Duration", "Project last Synced date", "Project Status", 
            "Certificate Status"
        ]
        execute_report("Project Status Exhaust", status_config, expected_headers_status, args.out_status_prefix, druid_url, pairs, dedup_uuid=True)

if __name__ == "__main__":
    main()
