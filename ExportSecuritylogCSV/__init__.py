import boto3
import csv
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from datetime import datetime

def main(mytimer):
    # ==============================
    # Configuración S3
    # ==============================
    BUCKET_NAME = "emcs-ssm"
    PREFIX = "TareasProgramadas/Logs_Export/"
    REGION = "eu-west-1"
    MAX_THREADS = 10

    s3 = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )

    all_results = []

    # ==============================
    # Obtener CommandIds
    # ==============================
    def list_command_ids():
        paginator = s3.get_paginator("list_objects_v2")
        command_ids = []

        for page in paginator.paginate(
            Bucket=BUCKET_NAME,
            Prefix=PREFIX,
            Delimiter="/"
        ):
            for p in page.get("CommonPrefixes", []):
                command_ids.append(p["Prefix"].replace(PREFIX, "").strip("/"))
        return command_ids

    # ==============================
    # Obtener InstanceIds
    # ==============================
    def list_instance_ids(command_id):
        path = f"{PREFIX}{command_id}/"
        paginator = s3.get_paginator("list_objects_v2")
        instance_ids = []

        for page in paginator.paginate(
            Bucket=BUCKET_NAME,
            Prefix=path,
            Delimiter="/"
        ):
            for p in page.get("CommonPrefixes", []):
                iid = p["Prefix"].replace(path, "").strip("/")
                if iid.startswith("i-"):
                    instance_ids.append(iid)
        return instance_ids

    # ==============================
    # Validar existencia de objeto
    # ==============================
    def object_exists(key):
        try:
            s3.head_object(Bucket=BUCKET_NAME, Key=key)
            return True
        except Exception:
            return False

    # ==============================
    # Leer archivo S3
    # ==============================
    def read_s3(key):
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            return obj["Body"].read().decode("utf-8", errors="ignore")
        except Exception:
            return ""

    # ==============================
    # Extraer metadata
    # ==============================
    def extract_metadata(text):
        account_name = ""
        execution_date = ""

        m_name = re.search(r"ACCOUNT NAME\s*:\s*(.+)", text)
        if m_name:
            account_name = m_name.group(1).strip()

        m_date = re.search(r"EXECUTION DATE\s*:\s*(.+)", text)
        if m_date:
            execution_date = m_date.group(1).strip()

        return account_name, execution_date

    # ==============================
    # Parsear fecha
    # ==============================
    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.min

    # ==============================
    # Procesar CommandId
    # ==============================
    def process_command(command_id):
        results = []
        instance_ids = list_instance_ids(command_id)

        for instance_id in instance_ids:
            base = f"{PREFIX}{command_id}/{instance_id}/awsrunPowerShellScript/0.awsrunPowerShellScript/"
            stdout_key = base + "stdout"
            stderr_key = base + "stderr"

            stdout = read_s3(stdout_key) if object_exists(stdout_key) else ""
            stderr = read_s3(stderr_key) if object_exists(stderr_key) else ""

            account_name, execution_date = extract_metadata(stdout)

            if stderr.strip():
                result = "ERROR"
                description = stderr.strip().replace("\r", " ").replace("\n", " ")[:1000]
            elif stdout.strip():
                result = "SUCCESS"
                description = stdout.strip().replace("\r", " ").replace("\n", " ")[:1000]
            else:
                result = "UNKNOWN"
                description = ""

            results.append([
                account_name,
                instance_id,
                execution_date,
                result,
                description
            ])
        return results

    # ==============================
    # Ejecución principal
    # ==============================
    command_ids = list_command_ids()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_command, cid) for cid in command_ids]
        for future in as_completed(futures):
            all_results.extend(future.result())

    # ==============================
    # Ordenar por fecha descendente
    # ==============================
    all_results.sort(key=lambda x: parse_date(x[2]), reverse=True)

    # ==============================
    # Crear CSV en memoria
    # ==============================
    output = StringIO()
    writer = csv.writer(output, delimiter=';')

    writer.writerow([
        "Cuenta",
        "Instancia",
        "Fecha de ejecucion",
        "Resultado",
        "Descripcion"
    ])
    writer.writerows(all_results)

    # ==============================
    # Subir a Azure Blob Storage
    # ==============================
    connection_string = os.environ["AzureWebJobsStorage"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_name = "copydatalog"

    container_client = blob_service_client.get_container_client(container_name)

    for blob in container_client.list_blobs():
        if blob.name.endswith(".csv"):
            container_client.delete_blob(blob.name)
            
    now_str = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    blob_name = f"ssm_logs_{now_str}.csv"

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(output.getvalue(), overwrite=True)

    print(f"CSV subido correctamente: {container_name}/{blob_name}")