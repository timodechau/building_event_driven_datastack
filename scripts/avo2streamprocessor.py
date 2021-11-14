import json

AVO_SCHEMA_FILE = '../tracking/AvoTrackingPlan.json'
SP_SCHEMA_FILE = '../tracking/streamprocessor_schema.avro'

BASE_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "com.deepskydata",
    "name": ""
}

def load_avo_schema(source_file: str):
    f = open(source_file)
    data = json.load(f)
    f.close()
    return data

def save_sp_schema(destination_file: str, data: dict):
    f = open(destination_file,"w")
    f.write(data)
    f.close()
    return None


def map_avo_to_streamprocessor(data: dict):
    events = data['events']
    sp_events = []
    for event in events:
        event_schema = BASE_AVRO_SCHEMA
        fields = []
        event_name = event.get('name','')
        event_schema['name'] = event_name
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
        sp_events.append(event_schema)
    return sp_events

avo_schema = load_avo_schema(AVO_SCHEMA_FILE)
sp_schema = map_avo_to_streamprocessor(avo_schema)
save_sp_schema(SP_SCHEMA_FILE,json.dumps(sp_schema))

        

