from __future__ import annotations
import argparse, json, sqlite3
from pathlib import Path
from src.lending_pipeline.generator import generate_data
from src.lending_pipeline.cli import run_pipeline
from src.lending_pipeline.settings import ROOT

def fetch_rows(db, mode):
    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT application_id,customer_id,loan_amount,application_status,age_at_application,loan_amount_category,application_timestamp,source_system FROM loan_applications WHERE processing_mode=? ORDER BY application_id",(mode,)).fetchall()
    conn.close(); return rows

def main():
    p=argparse.ArgumentParser(); p.add_argument("--customers",type=int,default=1000); p.add_argument("--applications",type=int,default=5000); p.add_argument("--seed",type=int,default=42); args=p.parse_args()
    data=ROOT/"data/generated/benchmark"; cp,ap=generate_data(data,args.customers,args.applications,args.seed)
    db=ROOT/"database/benchmark.db"
    base=run_pipeline(cp,ap,"BASELINE",db); opt=run_pipeline(cp,ap,"OPTIMIZED",db)
    base_rows=fetch_rows(db,"BASELINE"); opt_rows=fetch_rows(db,"OPTIMIZED")
    result={"dataset":{"customers":args.customers,"applications":args.applications,"seed":args.seed},"baseline":{k:v for k,v in base.items() if k!="physical_plan"},"optimized":{k:v for k,v in opt.items() if k!="physical_plan"},"business_outputs_equivalent":base_rows==opt_rows,"baseline_plan":base.get("physical_plan"),"optimized_plan":opt.get("physical_plan")}
    out=ROOT/"benchmark/results/latest_comparison.json"; out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(result,indent=2,default=str),encoding="utf-8")
    print(json.dumps({"result_file":str(out),"business_outputs_equivalent":result["business_outputs_equivalent"],"baseline_seconds":base.get("spark_duration_seconds"),"optimized_seconds":opt.get("spark_duration_seconds")},indent=2))
if __name__=="__main__": main()
