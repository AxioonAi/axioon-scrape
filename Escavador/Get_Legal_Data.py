from botocore.exceptions import ClientError

from datetime import datetime
import requests
import logging
import boto3
import json
import os



def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], region_name="us-east-1")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        acl = s3_client.put_object_acl(Bucket=bucket, Key=object_name, ACL='public-read')
    except ClientError as e:
        logging.error(e)
        return False
    return True

now = datetime.now()
timestamp = datetime.timestamp(now)

inputs = requests.get(f"{os.environ['API_IP']}/scrape/cpf")

inputs = json.loads(inputs.text)

for input in inputs:
    r = requests.get(f"https://api.escavador.com/api/v2/envolvido/processos?cpf_cnpj={input['cpf']}", headers={"Authorization": f"Bearer {os.environ['ESCAVADOR_TOKEN']}"})

    json_data = json.dumps(r.json())
    
    data = json.loads(json_data)
    
    data["user_id"] = input["id"]
    
    with open(f"/home/scrapeops/axioon-scrape/Results/{input['cpf']}.json", "w") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
        
    upload_file(f"/home/scrapeops/axioon-scrape/Results/{input['cpf']}.json", "axioon", f"Legal/{input['cpf']}_{timestamp}.json")
        
    file_name = requests.post(f"{os.environ['API_IP']}/webhook/legal", json={"records": f"Legal/{input['cpf']}_{timestamp}.json"})
