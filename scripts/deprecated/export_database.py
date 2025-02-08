import sqlite3
import os
import pandas as pd
from datetime import datetime

def get_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(os.path.dirname(current_dir), 'database', 'fraud_db.sqlite')
    return sqlite3.connect(db_path)

def export_database():
    with get_connection() as conn:
        # Get list of all tables
        tables = pd.read_sql("""
            SELECT name FROM sqlite_master 
            WHERE type='table'
            ORDER BY name;
        """, conn)
        
        # Create output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"database_export_{timestamp}.txt"
        
        with open(output_file, "w", encoding='utf-8') as f:
            f.write("=== DERIV FRAUD DETECTION DATABASE EXPORT ===\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Export each table
            for table_name in tables['name']:
                f.write(f"\n{'='*80}\n")
                f.write(f"TABLE: {table_name}\n")
                f.write(f"{'='*80}\n\n")
                
                # Get schema
                schema = pd.read_sql(f"PRAGMA table_info({table_name});", conn)
                f.write("SCHEMA:\n")
                f.write("-" * 80 + "\n")
                for _, row in schema.iterrows():
                    f.write(f"Column: {row['name']}\n")
                    f.write(f"Type: {row['type']}\n")
                    f.write(f"Nullable: {'Yes' if row['notnull'] == 0 else 'No'}\n")
                    f.write(f"Default: {row['dflt_value'] if row['dflt_value'] else 'None'}\n")
                    f.write(f"Primary Key: {'Yes' if row['pk'] == 1 else 'No'}\n")
                    f.write("-" * 40 + "\n")
                
                # Get all data
                data = pd.read_sql(f"SELECT * FROM {table_name};", conn)
                f.write(f"\nDATA ({len(data)} rows):\n")
                f.write("-" * 80 + "\n")
                
                if not data.empty:
                    # Write column headers
                    f.write("| " + " | ".join(str(col) for col in data.columns) + " |\n")
                    f.write("|" + "|".join("-" * len(str(col)) for col in data.columns) + "|\n")
                    
                    # Write data rows
                    for _, row in data.iterrows():
                        f.write("| " + " | ".join(str(val) for val in row) + " |\n")
                
                f.write("\n\n")
            
            # Write summary
            f.write(f"\n{'='*80}\n")
            f.write("DATABASE SUMMARY\n")
            f.write(f"{'='*80}\n\n")
            
            for table_name in tables['name']:
                count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name};", conn).iloc[0]['count']
                f.write(f"{table_name}: {count} rows\n")

        print(f"Database exported to {output_file}")
        return output_file

if __name__ == "__main__":
    export_database() 