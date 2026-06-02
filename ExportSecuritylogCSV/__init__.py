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
    PREFIX_TAREAS = "TareasProgramadas/"
    PREFIX_PATCH = "patch-manager/"
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
        entries = []

        # TareasProgramadas/ → subcarpeta → command_id
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX_TAREAS, Delimiter="/"):
            for p in page.get("CommonPrefixes", []):
                subcarpeta_prefix = p["Prefix"]
                for page2 in paginator.paginate(Bucket=BUCKET_NAME, Prefix=subcarpeta_prefix, Delimiter="/"):
                    for p2 in page2.get("CommonPrefixes", []):
                        command_id = p2["Prefix"].replace(subcarpeta_prefix, "").strip("/")
                        entries.append((subcarpeta_prefix, command_id, "tareas"))

        # patch-manager/ → cliente → command_id
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX_PATCH, Delimiter="/"):
            for p in page.get("CommonPrefixes", []):
                cliente_prefix = p["Prefix"]
                for page2 in paginator.paginate(Bucket=BUCKET_NAME, Prefix=cliente_prefix, Delimiter="/"):
                    for p2 in page2.get("CommonPrefixes", []):
                        command_id = p2["Prefix"].replace(cliente_prefix, "").strip("/")
                        entries.append((cliente_prefix, command_id, "patch"))

        return entries

    # ==============================
    # Obtener InstanceIds
    # ==============================
    def list_instance_ids(command_path):
        paginator = s3.get_paginator("list_objects_v2")
        instance_ids = []

        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=command_path, Delimiter="/"):
            for p in page.get("CommonPrefixes", []):
                iid = p["Prefix"].replace(command_path, "").strip("/")
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
        account_id = ""
        account_name = ""
        execution_date = ""
        instance_name = ""

        m_id = re.search(r"ACCOUNT ID\s*:\s*(.+)", text)
        if m_id:
            account_id = m_id.group(1).strip()

        m_name = re.search(r"ACCOUNT NAME\s*:\s*(.+)", text)
        if m_name:
            account_name = m_name.group(1).strip()

        m_date = re.search(r"EXECUTION DATE\s*:\s*(.+)", text)
        if m_date:
            execution_date = m_date.group(1).strip()

        m_instance = re.search(r"INSTANCE NAME\s*:\s*(.+)", text)
        if m_instance:
            instance_name = m_instance.group(1).strip()


        return account_name, str(account_id), execution_date, instance_name

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
    def process_command(entry):
        subcarpeta_prefix, command_id, tipo = entry
        results = []
      
        tarea = ""
        if tipo == "tareas":
            tarea = subcarpeta_prefix.replace(PREFIX_TAREAS, "").strip("/")

        command_path = f"{subcarpeta_prefix}{command_id}/"
        instance_ids = list_instance_ids(command_path)

        for instance_id in instance_ids:
            if tipo == "tareas":
                base = f"{command_path}{instance_id}/awsrunPowerShellScript/0.awsrunPowerShellScript/"
            else:  # patch
                base = f"{command_path}{instance_id}/awsrunPowerShellScript/PatchWindows/"

            stdout_key = base + "stdout"
            stderr_key = base + "stderr"

            stdout = read_s3(stdout_key) if object_exists(stdout_key) else ""
            stderr = read_s3(stderr_key) if object_exists(stderr_key) else ""

            account_name, account_id, execution_date, instance_name = extract_metadata(stdout if stdout.strip() else stderr)

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
                str(account_id),
                account_name,
                instance_id,
                instance_name,
                execution_date,
                result,
                tarea,
                description
            ])
        return results

    # ==============================
    # Ejecución principal
    # ==============================
    entries = list_command_ids()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_command, entry) for entry in entries]
        for future in as_completed(futures):
            all_results.extend(future.result())

    # ==============================
    # Ordenar por fecha descendente
    # ==============================
    all_results.sort(key=lambda x: parse_date(x[3]), reverse=True)

    # ==============================
    # Crear CSV en memoria
    # ==============================
    output = StringIO()
    writer = csv.writer(output, delimiter=';')

    writer.writerow([
        "id_cuenta",
        "Cuenta",
        "id_instancia",
        "Instancia",
        "Fecha de ejecucion",
        "Resultado",
        "Tarea",
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

