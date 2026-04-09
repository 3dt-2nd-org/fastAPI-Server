import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

def fetch_test_data(table_name: str):
    # .env 데이터 로드
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    catalog = os.getenv("DATABRICKS_CATALOG")
    schema = os.getenv("DATABRICKS_SCHEMA")

    try:
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                # 데이터 조회를 위한 전체 경로 설정
                full_table_path = f"{catalog}.{schema}.{table_name}"
                
                print(f"[*] {full_table_path} 테이블에서 데이터 조회를 시작합니다...")
                
                # 상위 5개 데이터만 조회 (테스트용)
                query = f"SELECT * FROM {full_table_path} LIMIT 5"
                cursor.execute(query)
                
                results = cursor.fetchall()

                if not results:
                    print("[-] 조회된 데이터가 없습니다.")
                else:
                    print(f"[+] 총 {len(results)}건의 데이터를 가져왔습니다.")
                    for row in results:
                        # row.asDict()를 사용하여 컬럼명과 값을 함께 확인
                        print(row.asDict())
                        
    except Exception as e:
        print(f"[!] 데이터 조회 실패: {e}")

if __name__ == "__main__":
    # 이전 단계에서 'SHOW TABLES'로 확인한 테이블 명 중 하나를 입력하세요
    target_table = "video_metadata" 
    fetch_test_data(target_table)