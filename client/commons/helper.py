import requests
import csv

def read_csv(file_path: str, headers: list):
    batch = []

    with open(file_path, mode='r', encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            batch.append(dict(zip(headers, row)))

    return batch

def send_request(url: str, batch: list):
    request = {
        'data' : batch
    }
    response  = requests.post(url=url, json=request)

    return response
