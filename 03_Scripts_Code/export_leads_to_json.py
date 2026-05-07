#!/usr/bin/env python3
"""
Export quality leads from the database to qualified_leads.json files for each city.
This allows the enrichment pipeline to process all leads, not just the sample 25 per city.
"""
import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime, timezone

# Database configuration for local Docker container
DB_HOST = "localhost"
DB_PORT = 6543
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "supabase_service_1777905407"

# Target cities (same as in abn_enrichment.py)
CITIES = ["sydney", "melbourne", "brisbane", "perth", "adelaide", "hobart", "darwin", "canberra"]

def connect_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def export_leads():
    conn = connect_db()
    cur = conn.cursor()
    
    # Query to get all quality leads (lead_score >= 55) in target industries
    # Group by city
    for city in CITIES:
        query = """
        SELECT 
            lead_id,
            business_name,
            category,
            subcategory,
            services,
            phone,
            mobile,
            email,
            website,
            city,
            state,
            suburb,
            postcode,
            address_full,
            source,
            abn,
            abn_status,
            lead_score,
            needs_review,
            enriched_at,
            about_short,
            hero_headline,
            variation
        FROM leads 
        WHERE city = %s 
          AND lead_score >= 55 
          AND category IN ('plumbing', 'electrical', 'builder', 'painter', 'carpenter', 
                          'roofer', 'solar', 'air conditioning', 'flooring', 'kitchen',
                          'mechanic', 'pest control')
        ORDER BY lead_score DESC;
        """
        cur.execute(query, (city,))
        rows = cur.fetchall()
        
        leads = []
        for row in rows:
            lead = {
                "id": row[0],
                "business_name": row[1],
                "category": row[2],
                "subcategory": row[3],
                "services": row[4],
                "phone": row[5],
                "mobile": row[6],
                "email": row[7],
                "website": row[8],
                "city": row[9],
                "state": row[10],
                "suburb": row[11],
                "postcode": row[12],
                "address_full": row[13],
                "source": row[14],
                "abn": row[15],
                "abn_status": row[16],
                "lead_score": row[17],
                "needs_review": row[18],
                "enriched_at": row[19],
                "about_short": row[20],
                "hero_headline": row[21],
                "variation": row[22]
            }
            leads.append(lead)
        
        # Write to JSON file
        output_dir = Path("/home/thinkpad/Projects/active/project-WEBTEST") / city / "raw_leads"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "qualified_leads.json"
        
        with open(output_file, "w") as f:
            json.dump(leads, f, indent=2)
        
        print(f"Exported {len(leads)} leads for {city}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    export_leads()