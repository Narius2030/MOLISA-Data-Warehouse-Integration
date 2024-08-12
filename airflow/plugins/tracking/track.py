import psycopg2 
import json

with open("/opt/airflow/config.json", "r") as file:
    config = json.load(file)

def track_refresh():
    with psycopg2.connect(
        database="LdtbxhStage",
        user=f"{config['USER']}",
        password=f"{config['PASSWORD']}",
        host=f"{config['HOST_DOCKER']}",
        port=f"{config['PORT']}"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                            INSERT INTO "DimAuditForeigned"(process_name, start_at, finished_at, information, status) VALUES
                            ('data integration', NOW(), NULL, 'refreshed data', 'PENDING')
                        """)
            
def track_transform():
    with psycopg2.connect(
        database="LdtbxhStage",
        user=f"{config['USER']}",
        password=f"{config['PASSWORD']}",
        host=f"{config['HOST_DOCKER']}",
        port=f"{config['PORT']}"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                            UPDATE "DimAuditForeigned"
                            SET
                                process_name = 'data integration', 
                                finished_at = NOW(),
                                information = 'transformed successfully'
                            WHERE start_at = (SELECT MAX(start_at) FROM "DimAuditForeigned")
                        """)
            
def track_load():
    with psycopg2.connect(
        database="LdtbxhStage",
        user=f"{config['USER']}",
        password=f"{config['PASSWORD']}",
        host=f"{config['HOST_DOCKER']}",
        port=f"{config['PORT']}"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                            UPDATE "DimAuditForeigned"
                            SET
                                process_name = 'data integration', 
                                finished_at = NOW(),
                                information = 'ETL successfully',
                                status = 'SUCCESS'
                            WHERE start_at = (SELECT MAX(start_at) FROM "DimAuditForeigned")
                        """)
            
if __name__ == '__main__':
    print('')