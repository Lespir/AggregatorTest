import motor.motor_asyncio
from datetime import datetime


async def aggregate_salaries(dt_from, dt_upto, group_type):
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["Aggregator"]
    collection = db["wages"]

    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    date_range = {
        '$gte': dt_from,
        '$lte': dt_upto
    }

    # Определите, как группировать данные
    group_format = '%Y-%m-%d'
    if group_type == 'hour':
        group_format = '%Y-%m-%d %H:00:00'
    elif group_type == 'month':
        group_format = '%Y-%m-01'

    pipeline = [
        {
            '$match': {
                'dt': date_range
            }
        },
        {
            '$group': {
                '_id': {
                    '$dateToString': {
                        'format': group_format,
                        'date': '$dt'
                    }
                },
                'total': {
                    '$sum': '$value'
                }
            }
        },
        {
            '$sort': {
                '_id': 1
            }
        }
    ]

    out_docs = await collection.aggregate(pipeline).to_list(None)

    dataset = [item['total'] for item in out_docs]
    labels = [item['_id'] for item in out_docs]

    return dataset, labels
