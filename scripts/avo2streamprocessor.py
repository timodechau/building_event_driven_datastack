import json
import os

from slugify import slugify
from dotenv import load_dotenv

from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub import SchemaServiceClient
from google.pubsub_v1.types import Schema

load_dotenv()

AVO_SCHEMA_FILE = '../trackingplan/AvoTrackingPlan.json'
SP_SCHEMA_FILE = '../schema/streamprocessor_schema_'
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']

BASE_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "com.deepskydata.",
    "name": "Avro"
}

def load_avo_schema(source_file: str):
    f = open(source_file)
    data = json.load(f)
    f.close()
    return data

def save_sp_schema(destination_file: str, data: str):
    f = open(destination_file,"w")
    f.write(data)
    f.close()
    return None


def map_avo_to_avroschema(data: dict):
    events = data['events']
    elist = []
    for event in events:
        event_schema = BASE_AVRO_SCHEMA.copy()
        fields = []
        event_name = event.get('name','')
        event_schema['namespace'] = event_schema['namespace'] + slugify(event_name,separator='_')
        fields.append({
            "name":"event_name",
            "type":"string",
            "default":event_name,
            "doc":"{} -- avo_id:{}".format(event.get("description",""),event.get("id",""))
        })
        
        properties = event.get('rules',{}).get('properties',{}).get('properties',{}).get('properties',{})
        for item in properties.items():
            property_name = item[0]
            data = item[1]
            fields.append({
                "name":property_name,
                "type":"string",
                "doc":"{} -- avo_id:{}".format(data.get("description",""),data.get("id",""))
            })
        event_schema['fields'] = fields
        elist.append(event_schema)
        
    return elist

def post_schema_to_gcp_pubsub(avsc_file,namespace):
    project_path = f"projects/{GCP_PROJECT_ID}"

    with open(avsc_file, "rb") as f:
        avsc_source = f.read().decode("utf-8")



    schema_client = SchemaServiceClient()
    schema_path = schema_client.schema_path(GCP_PROJECT_ID, namespace)
    schema = Schema(name=schema_path, type_=Schema.Type.AVRO, definition=avsc_source)
    try:
        result = schema_client.create_schema(
            request={"parent": project_path, "schema": schema, "schema_id": namespace}
        )
        print(f"Created a schema using an Avro schema file:\n{result}")
    except AlreadyExists:
        print(f"{namespace} already exists.")


avo_schema = load_avo_schema(AVO_SCHEMA_FILE)
sp_schemas = map_avo_to_avroschema(avo_schema)
for schema in sp_schemas:
    file_name = SP_SCHEMA_FILE + schema['namespace'] + ".avsc"
    save_sp_schema(file_name,json.dumps(schema,indent=4))
    post_schema_to_gcp_pubsub(file_name,schema['namespace'])
#test_validate_avro_data(sp_schema)
#print(sp_schema)
#save_sp_schema(SP_SCHEMA_FILE,json.dumps(sp_schema))

        

