import duckdb
import os
import csv
from pathlib import Path

def save_schema_to_csv(conn, table_name, output_file):
    # Create schemas directory if it doesn't exist
    Path('schemas').mkdir(exist_ok=True)
    
    try:
        # Get first row of data
        result = conn.execute(f"""
            SELECT *
            FROM {table_name}
            LIMIT 1
        """).fetchone()
        
        if result is None:
            print(f"No data found in {table_name}")
            return
        
        # Get column names
        columns = conn.execute(f"DESCRIBE SELECT * FROM {table_name}").fetchall()
        column_names = [col[0] for col in columns]
        
        # Create schema file with column names and example values
        with open(f'schemas/{output_file}', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Column Name', 'Example Value'])
            
            for col_name, value in zip(column_names, result):
                if value is not None:
                    writer.writerow([col_name, value])
    
    except Exception as e:
        print(f"An error occurred while saving schema: {e}")

def get_parquet_info():
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Create input_schemas directory if it doesn't exist
    Path('input_schemas').mkdir(exist_ok=True)
    
    # URL of the Parquet file
    parquet_url = "https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet"
    
    try:
        # Create a small sample locally (taking just 100 rows)
        print("Creating local sample...")
        conn.execute("""
            CREATE TABLE food_sample AS 
            SELECT * 
            FROM read_parquet($1) 
            LIMIT 100
        """, [parquet_url])
        
        # Save the sample as a local CSV file in input_schemas
        print("Saving sample to input_schemas/food_sample.csv...")
        conn.execute("""
            COPY food_sample TO 'input_schemas/food_sample.csv' (FORMAT CSV, HEADER)
        """)
        
        # Get column names from the sample
        result = conn.execute("DESCRIBE food_sample").fetchall()
        
        # Extract and print column names
        column_names = [row[0] for row in result]
        print("\nColumns in the dataset:")
        for i, col in enumerate(column_names, 1):
            print(f"{i}. {col}")
            
        # Save original schema
        save_schema_to_csv(conn, 'food_sample', 'original_schema.csv')
        
        return column_names
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    finally:
        conn.close()

def print_product_info(code="0000101209159"):
    # Connect to DuckDB
    conn = duckdb.connect()
    
    try:
        # Try to read from local sample first
        print(f"\nLooking up product code: {code}")
        result = conn.execute("""
            SELECT *
            FROM read_csv('input_schemas/food_sample.csv')
            WHERE code = $1
        """, [code]).fetchone()
        
        if result is None:
            print(f"Product {code} not found in local sample.")
            return
        
        # Get column names
        columns = conn.execute("DESCRIBE SELECT * FROM read_csv('input_schemas/food_sample.csv')").fetchall()
        column_names = [col[0] for col in columns]
        
        # Print each property and its value
        print("\nProduct properties:")
        for col_name, value in zip(column_names, result):
            if value is not None and str(value).strip() != '':
                print(f"{col_name}: {value}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        conn.close()

def print_first_row(csv_file='input_schemas/food_sample_target.csv'):
    # Connect to DuckDB
    conn = duckdb.connect()
    
    try:
        # Get the first row
        print(f"\nReading first row from {csv_file}")
        result = conn.execute(f"""
            SELECT *
            FROM read_csv('{csv_file}')
            LIMIT 1
        """).fetchone()
        
        if result is None:
            print(f"No data found in {csv_file}")
            return
        
        # Get column names
        columns = conn.execute(f"DESCRIBE SELECT * FROM read_csv('{csv_file}')").fetchall()
        column_names = [col[0] for col in columns]
        
        # Print each property and its value
        print("\nFirst row properties:")
        for col_name, value in zip(column_names, result):
            if value is not None and str(value).strip() != '':
                print(f"{col_name}: {value}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        conn.close()

def transform_and_save_sample():
    # Connect to DuckDB
    conn = duckdb.connect()
    
    try:
        print("Transforming data to match target schema...")
        conn.execute("""
            CREATE TABLE transformed_sample AS
            SELECT 
                CAST(code AS DOUBLE) as code,
                'http://world-en.openfoodfacts.org/product/' || code as url,
                creator,
                CAST(created_t AS BIGINT) as created_t,
                created_t::STRING as created_datetime,
                CAST(last_modified_t AS BIGINT) as last_modified_t,
                last_modified_t::STRING as last_modified_datetime,
                last_modified_by,
                -- Handle product name more safely
                CASE 
                    WHEN product_name IS NOT NULL AND product_name != ''
                    THEN COALESCE(TRY_CAST(product_name AS JSON), '[]')::JSON->0->>'text'
                    ELSE ''
                END as product_name,
                quantity,
                -- Handle packaging more safely
                CASE 
                    WHEN packagings IS NOT NULL AND packagings != ''
                    THEN COALESCE(TRY_CAST(packagings AS JSON), '[]')::JSON->0->>'material'
                    ELSE ''
                END as packaging,
                packaging_tags,
                -- Simplified packaging
                CASE 
                    WHEN packagings IS NOT NULL AND packagings != ''
                    THEN REPLACE(COALESCE(TRY_CAST(packagings AS JSON), '[]')::JSON->0->>'material', 'en:', '')
                    ELSE ''
                END as packaging_en,
                brands,
                brands_tags,
                categories,
                categories_tags,
                -- Extract only English categories and remove 'en:' prefix
                CASE 
                    WHEN categories_tags IS NOT NULL AND categories_tags != ''
                    THEN list_transform(
                        list_filter(
                            string_split(TRIM(BOTH '[]' FROM categories_tags), ','), 
                            x -> TRIM(x) LIKE 'en:%'
                        ),
                        x -> '"' || TRIM(REPLACE(TRIM(x), 'en:', '')) || '"'
                    )::STRING
                    ELSE '[]'
                END as categories_en,
                '' as origins,
                origins_tags,
                origins_tags as origins_en,
                manufacturing_places,
                manufacturing_places_tags,
                labels,
                labels_tags,
                -- Extract only English labels
                CASE 
                    WHEN labels_tags IS NOT NULL AND labels_tags != ''
                    THEN list_transform(
                        list_filter(
                            string_split(TRIM(BOTH '[]' FROM labels_tags), ','), 
                            x -> TRIM(x) LIKE 'en:%'
                        ),
                        x -> '"' || TRIM(REPLACE(TRIM(x), 'en:', '')) || '"'
                    )::STRING
                    ELSE '[]'
                END as labels_en,
                emb_codes,
                emb_codes_tags,
                'en:france' as countries,
                countries_tags,
                -- Extract only English countries
                CASE 
                    WHEN countries_tags IS NOT NULL AND countries_tags != ''
                    THEN list_transform(
                        list_filter(
                            string_split(TRIM(BOTH '[]' FROM countries_tags), ','), 
                            x -> TRIM(x) LIKE 'en:%'
                        ),
                        x -> '"' || TRIM(REPLACE(TRIM(x), 'en:', '')) || '"'
                    )::STRING
                    ELSE '[]'
                END as countries_en,
                COALESCE(ingredients_text, '') as ingredients_text,
                0 as additives_n,
                -- Handle JSON arrays more safely
                CASE 
                    WHEN nova_groups_tags IS NOT NULL AND nova_groups_tags != ''
                    THEN COALESCE(TRY_CAST(nova_groups_tags AS JSON), '[]')::JSON->0
                    ELSE ''
                END as nova_group,
                'unknown' as pnns_groups_1,
                'unknown' as pnns_groups_2,
                CASE 
                    WHEN food_groups_tags IS NOT NULL AND food_groups_tags != ''
                    THEN list_transform(
                        list_filter(
                            string_split(TRIM(BOTH '[]' FROM food_groups_tags), ','), 
                            x -> TRIM(x) LIKE 'en:%'
                        ),
                        x -> '"' || TRIM(REPLACE(TRIM(x), 'en:', '')) || '"'
                    )::STRING
                    ELSE '[]'
                END as food_groups_en,
                food_groups_tags,
                states_tags,
                states_tags,
                -- Extract only English states
                CASE 
                    WHEN states_tags IS NOT NULL AND states_tags != ''
                    THEN list_transform(
                        list_filter(
                            string_split(TRIM(BOTH '[]' FROM states_tags), ','), 
                            x -> TRIM(x) LIKE 'en:%'
                        ),
                        x -> '"' || TRIM(REPLACE(TRIM(x), 'en:', '')) || '"'
                    )::STRING
                    ELSE '[]'
                END as states_en,
                CAST(ecoscore_score AS INTEGER) as ecoscore_score,
                ecoscore_grade,
                -- Add stores fields
                stores,
                stores_tags,
                -- Transform stores (no en: prefix filtering needed)
                CASE 
                    WHEN stores_tags IS NOT NULL AND stores_tags != ''
                    THEN list_transform(
                        string_split(TRIM(BOTH '[]' FROM stores_tags), ','),
                        x -> '"' || TRIM(x) || '"'
                    )::STRING
                    ELSE '[]'
                END as stores_en,
                -- Handle quantity conversion more safely
                CASE 
                    WHEN quantity IS NOT NULL AND quantity != ''
                    THEN TRY_CAST(REGEXP_REPLACE(quantity, '[^0-9]', '') AS INTEGER)
                    ELSE 0
                END as product_quantity,
                0 as unique_scans_n,
                completeness,
                CAST(last_image_t AS BIGINT) as last_image_t,
                last_image_t::STRING as last_image_datetime
            FROM read_csv('input_schemas/food_sample.csv')
        """)
        
        # Save the transformed data
        print("Saving transformed data to input_schemas/food_sample_transformed.csv...")
        conn.execute("""
            COPY transformed_sample TO 'input_schemas/food_sample_transformed.csv' 
            (FORMAT CSV, HEADER)
        """)
        
        # Save transformed schema
        save_schema_to_csv(conn, 'transformed_sample', 'transformed_schema.csv')
        
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        conn.close()

def save_target_schema():
    conn = duckdb.connect()
    try:
        # Save target schema from input_schemas directory
        save_schema_to_csv(conn, "read_csv('input_schemas/food_sample_target.csv')", 'target_schema.csv')
    finally:
        conn.close()

if __name__ == "__main__":
    # First ensure we have the source data
    if not os.path.exists('input_schemas/food_sample.csv'):
        print("Creating initial sample file...")
        get_parquet_info()
    
    transform_and_save_sample()
    save_target_schema()
    print_first_row('input_schemas/food_sample_transformed.csv') 