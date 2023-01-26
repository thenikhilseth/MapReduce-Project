import math
from pymongo import MongoClient
import pprint

# metric_id = input('Please Enter Request For Workload (RFW) ID:')
# bench_type = input('Please type one of the following:\n 1. DVD\n 2. NDBench\n')
# metric = input('Please type one of the metrics from the following:\n'
#                '1. CPUUtilization_Average\n 2. NetworkIn_Average\n 3. NetworkOut_Average\n'
#                ' 4. MemoryUtilization_Average\n')
# batch_id = int(input(
#     'Please Enter the Batch Id (from which batch you want the data to start from) in integer: '))
# batch_unit = int(
#     input('Please Enter the number of samples you want in one batch in integer: '))
# batch_size = int(
#     input('Please Enter the number of batches to be returned in integer: '))
# data_type = input(
#     'Please type one of the following:\n 1. testing\n 2. training\n')

metric_id = '1'
bench_type = "DVD"
metric = "CPUUtilization_Average"
batch_id = 2
batch_unit = 3
batch_size = 2
data_type = "testing"


def connect_database():

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb+srv://benazir:2concordia1!@cluster0.qognszm.mongodb.net/test"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    return client


if __name__ == "__main__":
    client = connect_database() 
    db = client["MongoDatabase"]
    col_name = str(bench_type)+"-"+str(data_type)
    coll = db[col_name]
    total_length = len(list(coll.find()))
    bucket = math.ceil(total_length/batch_unit)
    pipeline = [
        {
            "$bucketAuto": {
                "groupBy":  '$_id',
                "buckets": bucket,
                "output": {
                            "batch": {
                                "$push":  '$'+metric
                            } 
                }
            }
        },
        {
            "$skip": batch_id
        },
        {
            "$limit": batch_size
        },

        {
            '$project': {
                "batch": 1,
                'minimum': {
                    '$min': '$batch'
                },
                'maximum': {
                    '$max': '$batch'
                },
                'standard-deviation': {
                    '$stdDevPop': '$batch'
                }
            }
        },
        {
            '$project': {
                'metric_id': metric_id,
                'minimum': 1,
                'maximum': 1,
                'batch': 1,
                'standard-deviation': 1,
                "size": {"$size": "$batch"}
            }
        },
        {
            "$project": {
                'metric_id': metric_id,
                'minimum': 1,
                'maximum': 1,
                'standard-deviation': 1,
                "batch": 1,
                "checkEvenLength": {"$eq": [{"$mod": ["$size", 2]}, 0]},
                "middleIndex": {"$trunc": {"$divide": ["$size", 2]}}
            }
        },
        {
            "$project": {
                'metric_id': metric_id,
                'minimum': 1,
                'maximum': 1,
                'standard-deviation': 1,
                "batch": 1,
                "checkEvenLength": 1,
                "middleIndex": 1,
                "startMiddleIndex": {"$subtract": ["$middleIndex", 1]},
                "endMiddleIndex": "$middleIndex"
            }
        },
        {
            "$project": {
                'metric_id': metric_id,
                'minimum': 1,
                'maximum': 1,
                'standard-deviation': 1,
                "batch": 1,
                "checkEvenLength": 1,
                "middleIndex": 1,
                "startMedianValue": {"$arrayElemAt": ["$batch", "$startMiddleIndex"]},
                "endMedianValue": {"$arrayElemAt": ["$batch", "$endMiddleIndex"]},
                "isEvenLength": 1
            }
        },
        {
            "$project": {
                'metric_id': metric_id,
                'minimum': 1,
                'maximum': 1,
                'standard-deviation': 1,
                "batch": 1,
                "checkEvenLength": 1,
                "middleIndex": 1,
                "middleSum": {"$add": ["$startMedianValue", "$endMedianValue"]},
                "isEvenLength": 1
            }
        },
        {
            "$project": {
                '_id':0,
                'metric_id': metric_id,
                'minimum': 1,
                'maximum': 1,
                # "batch": 1, #Used to check whether the data sets are balanced.
                'standard-deviation': 1,
                'median': {
                    "$cond": {
                        'if': {'$eq': ['$checkEvenLength', True]},
                        'then': {"$divide": ["$middleSum", 2]},
                        'else':  {"$arrayElemAt": ["$batch", "$middleIndex"]}
                    }
                }
            }
        }
    ]
    result = coll.aggregate(pipeline)
    pprint.pprint(list(result))


#EXPLAIN COMMAND

    #explain command (Run when you want to see the file generated)
    # explain_output = db.command(
    #     'aggregate', col_name, pipeline=pipeline, explain=True)
    # pprint.pprint(explain_output)

