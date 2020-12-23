import boto3
import datetime

def handler(event, context):
    '''
    This Lambda is used to update the model registry.
    Ordinarily it would not be managed from within this pipeline as it would be used by all pipelines,
    but for now we will put the code in this step function deployment script
    '''

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('model_registry')
    
    today = datetime.datetime.now()
    dateAsString = today.strftime('%Y%m%d%H%M') 

    Item = {
        'run_id' : event["run_id"],
        'timestamp' : int(dateAsString),
        'environment': event["environment"],
        'algorithm': event["algorithm"],
        'model_location' : event["model_location"]
    }
    
    table.put_item(Item = Item)

    

# used for testing
if __name__ == "__main__":
    event = {
        'run_id' : "ab01",
        'environment': "DEV",
        'algorithm': "xgboost",
        'model_location' : 's3://somebucket/somemodel.tar'
    }
    handler(event)


