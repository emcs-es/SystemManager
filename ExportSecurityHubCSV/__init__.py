import boto3
import csv
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient
from datetime import datetime

def main(mytimer):

    # ==============================
    # Cliente de Security Hub
    # ==============================
    client = boto3.client(
       'securityhub',
        region_name='eu-west-1',
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )

    all_findings = []
    next_token = None

    while True:
        params = {
            'Filters': {
                'SeverityLabel': [
                    {'Value': 'CRITICAL', 'Comparison': 'EQUALS'},
                    {'Value': 'HIGH', 'Comparison': 'EQUALS'}
                ]
            },
            'MaxResults': 100
        }

        if next_token:
            params['NextToken'] = next_token

        response = client.get_findings(**params)
        all_findings.extend(response['Findings'])

        next_token = response.get('NextToken')
        if not next_token:
            break

    print(f"Total findings obtenidos: {len(all_findings)}")

    # ==============================
    # Crear CSV en memoria
    # ==============================
    output = StringIO()
    writer = csv.writer(output)

    writer.writerow([
        'id_cuenta',
        'Fecha',
        'Criticidad',
        'Clasificacion',
        'Cuenta',
        'Descripcion',
        'Recurso',
        'Estado',
        'Correcion'
    ])

    for f in all_findings:

        account_id = f.get('AwsAccountId', '')
        fecha = f.get('UpdatedAt', '')
        gravedad = f.get('Severity', {}).get('Label', '')
        estado = f.get('Workflow', {}).get('Status', '')
        accountname = f.get('AwsAccountName', '')
        descripcion = f.get('Description', '')
        recursos = f.get('Resources', [])
        recurso = recursos[0].get('Id', '') if recursos else ''

        compliance_status = (
            f.get('Compliance', {})
             .get('Status', '')
        )

        http = (
            f.get('Remediation', {})
             .get('Recommendation', {})
             .get('Url', '')
        )

        writer.writerow([
            account_id,
            fecha,
            gravedad,
            estado,
            accountname,
            descripcion,
            recurso,
            compliance_status,
            http
        ])

    # ==============================
    # Subir a Blob Storage
    # ==============================
    
    connection_string = os.environ["AzureWebJobsStorage"]

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )

    container_name = "copydataexport"

    container_client = blob_service_client.get_container_client(
        container_name
    )

    for blob in container_client.list_blobs():
        if blob.name.endswith(".csv"):
            container_client.delete_blob(blob.name)

    now_str = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    blob_name = f"findings_{now_str}.csv"

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    
    blob_client.upload_blob(output.getvalue(), overwrite=True)

    print(f"CSV subido a {container_name}/{blob_name}")