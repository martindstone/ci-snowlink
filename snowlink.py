import re
import json
import pysnow
import pd
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("snow_instance", help="ServiceNow instance name (just the first part of the hostname)")
parser.add_argument("snow_admin_user", help="ServiceNow administrator username")
parser.add_argument("snow_admin_pass", help="ServiceNow administrator password")
parser.add_argument("snow_pd_user", help="ServiceNow PD Integration username")
parser.add_argument("snow_pd_pass", help="ServiceNow PD Integration password")
parser.add_argument("pd_api_key", help="PagerDuty API key")
args = parser.parse_args()

pd_extension_schema_id = 'PBZUP2B'
pd_incident_sync_mode = 'sync_all'

pd_services = pd.fetch_services(api_key=args.pd_api_key)
print(f"Found {len(pd_services)} PD services")

pd_extensions = pd.fetch(api_key=args.pd_api_key, endpoint="extensions")
pd_extensions = list(filter(lambda x: x['extension_schema']['id'] == pd_extension_schema_id, pd_extensions))
print(f"Found {len(pd_extensions)} PD SNOW v6 extensions")

pd_extensions_by_service = {}
for pd_extension in pd_extensions:
    for extension_object in pd_extension['extension_objects']:
        if extension_object['type'] == 'service_reference':
            pd_extensions_by_service[extension_object['id']] = pd_extension

# Create SNOW client object
c = pysnow.Client(instance=args.snow_instance, user=args.snow_admin_user, password=args.snow_admin_pass)
cmdb = c.resource(api_path='/table/cmdb_ci')

def create_pd_snow_extension(pd_service_id):
    create_extension_body = {
        "extension":
        {
            "name": f"ServiceNow ({args.snow_instance})",
            "config": {
                "snow_user": args.snow_pd_user,
                "snow_password": args.snow_pd_pass,
                "sync_options": pd_incident_sync_mode,
                "target": f"https://{args.snow_instance}.service-now.com/api/x_pd_integration/pagerduty2sn"
            },
            "extension_schema": {
                "id": pd_extension_schema_id,
                "type": "extension_schema_reference"
            },
            "extension_objects": [
                {
                    "id": f"{pd_service_id}",
                    "type": "service_reference"
                }
            ]
        }
    }
    r = pd.request(api_key=args.pd_api_key, endpoint="extensions", method="POST", data=create_extension_body)
    return r["extension"]["id"]

for service in pd_services:
    # clean off leading SN: from the name if it exists
    service_name = re.sub(r'^SN:', '', service['name'])
    response = cmdb.get(query={'name': service_name})
    if len(response.all()) > 0:
        print(f"Found {len(response.all())} CMDB records for service name {service_name}")
        if len(response.all()) > 1:
            print("  ... that's too many; skipping")
            continue
        ci = response.all()[0]
        ci_sys_id = ci.get("sys_id")

        pd_service_id = ci.get("x_pd_integration_pagerduty_service")
        pd_webhook_id = ci.get("x_pd_integration_pagerduty_webhook")
        pd_extension = pd_extensions_by_service.get(service["id"])

        if pd_service_id and pd_webhook_id:
            print(f"  It's already got service {pd_service_id} and webhook {pd_webhook_id}, skipping...")
            if pd_service_id != service['id']:
                print(f"    Note: Service ID doesn't match (PD has {service['id']}, SNOW has {pd_service_id}")
            if not pd_extension:
                print(f"    Note: SNOW CMDB record's PD webhook ID {pd_webhook_id} wasn't found in PD")
            continue

        if pd_extension:
            print(f"  There's already a webhook in PD with ID {pd_extension['id']}")
            pd_webhook_id = pd_extension['id']
        else:
            pd_webhook_id = create_pd_snow_extension(service["id"])
            print(f"  Created PD webhook {pd_webhook_id}")

        snow_update_payload = {
            "x_pd_integration_pagerduty_service": service["id"],
            "x_pd_integration_pagerduty_webhook": pd_webhook_id
        }
        r = cmdb.update(query={"sys_id": ci_sys_id}, payload=snow_update_payload)
        print(f"  Set x_pd_integration_pagerduty_service to {r['x_pd_integration_pagerduty_service']} and x_pd_integration_pagerduty_webhook to {r['x_pd_integration_pagerduty_webhook']}")
