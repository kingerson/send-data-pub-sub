import base64
import json
import os
from datetime import datetime, timedelta
from google.cloud import bigquery


dataset = "realplaza_sw_notification_hub_uat"


def get_notification_state(state):
    if(state == 0):
        return "No Entregado"
    elif(state == 1):
        return "Entregado"
    elif(state == 2):
        return "Recibido"
    else:
        return "Abierto"


def register_row_to_bq(rows, table):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    table = client.get_table(table_ref)
    errors = client.insert_rows(table, rows)
    if not errors:
        print("Se registro correctamente la información.")
    else:
        print('Errors:')
        for error in errors:
            print(error)


def insert_notifications(event, context):
    print("""This Function was triggered by messageId {} published at {}""".format(
        context.event_id, context.timestamp))
    data = json.loads(base64.b64decode(event["data"]).decode('utf-8'))

    if data["type"] == "NC":
        rows = [{
            "rp_notification_id": data["rp_notification_id"],
            "rp_sequential_id": data["rp_sequential_id"],
            "message": data["message"],
            "uuid":  data["uuid"],
            "register_date": data["register_date"],
            "state": get_notification_state(data["state"]),
            "rp_register_timestamp": datetime.now() - timedelta(hours=5)
        }]
        register_row_to_bq(rows, 'tb_sending_notification')
    elif data["type"] == "NM":
        rows = [{
            "rp_sequential_id": data["rp_sequential_id"],
            "rp_id": data["rp_id"],
            "quantity": data["quantity"],
            "campaign_id": data["campaign_id"],
            "campaign": data["campaign"],
            "application_id": data["application_id"],
            "application": data["application"],
            "message": data["message"],
            "rp_register_timestamp": datetime.now() - timedelta(hours=5)
        }]
        register_row_to_bq(rows, 'tb_sendig_notification_batch')
