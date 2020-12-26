import uuid
import logging
import stepfunctions
import boto3
import sagemaker

from sagemaker.amazon.amazon_estimator import get_image_uri
from sagemaker import s3_input
from sagemaker.s3 import S3Uploader
from stepfunctions import steps
from stepfunctions.steps import TrainingStep, ModelStep
from stepfunctions.inputs import ExecutionInput
from stepfunctions.workflow import Workflow

import os

import datetime

# get a unique identifier for the job
# using a timestamp from the launch of the step functions could be a better option
import uuid
id = uuid.uuid4().hex

session = sagemaker.Session()
bucket = session.default_bucket()
# in this example it needs to be eu-west-1
region = 'eu-west-1' # boto3.Session().region_name

# these job names and function names were autogenerated on first run
# we are maintaining these names in order to replace the old versions of the
# lambda and glue job with new versions
job_name = 'glue-customer-churn-etl-c020134fb5334562bb3c31e6d02cc77d'
function_name = 'arn:aws:lambda:eu-west-1:963778699255:function:query-training-status-c020134fb5334562bb3c31e6d02cc77d'
workflow_name = 'MyInferenceRoutine_c020134fb5334562bb3c31e6d02cc77d'

#today = datetime.datetime.now()
#dateAsString = today.strftime('%Y%m%d%H%M') 

training_job_name = "CustomerChurnTrainingJob" + id


# specify the roles that will be used by the various artifacts
workflow_execution_role = os.getenv('workflow_execution_role')
sagemaker_execution_role = os.getenv('sagemaker_execution_role')
glue_role = os.getenv('glue_role')
lambda_role = os.getenv('lambda_role')
registry_lambda_role= os.getenv('model_registry_lambda_role')


# normally the data would already be there. In this example we are uploading it. 
# This can be removed for a real project
project_name = 'customer-churn-' + id

data_source = S3Uploader.upload(local_path='./data/customer-churn.csv',
                               desired_s3_uri='s3://{}/{}'.format(bucket, project_name),
                               session=session)

train_prefix = 'train'
val_prefix = 'validation'

train_data = 's3://{}/{}/{}/'.format(bucket, project_name, train_prefix)
validation_data = 's3://{}/{}/{}/'.format(bucket, project_name, val_prefix)


glue_script_location = S3Uploader.upload(local_path='./code/glue_etl.py',
                               desired_s3_uri='s3://{}/{}'.format(bucket, project_name),
                               session=session)
glue_client = boto3.client('glue')

## Updating existing job rather than creating a new one
# Might want to change this so it deletes the original and then creates a new one
response = glue_client.update_job( # change to create job if first time
    JobName=job_name,
    JobUpdate = {
        'Description':'PySpark job to extract the data and split in to training and validation data sets',
        'Role':glue_role, # you can pass your existing AWS Glue role here if you have used Glue before
        'ExecutionProperty':{
            'MaxConcurrentRuns': 2
        },
        'Command':{
            'Name': 'glueetl',
            'ScriptLocation': glue_script_location,
            'PythonVersion': '3'
        },
        'DefaultArguments':{
            '--job-language': 'python'
        },
        'GlueVersion':'1.0',
        'WorkerType':'Standard',
        'NumberOfWorkers':2,
        'Timeout':3 # changed the timeout to 3 minutes
    }
)

# Create the Lambda that checks for the quality
import zipfile
zip_name = 'query_training_status.zip'
lambda_source_code = './code/query_training_status.py'

zf = zipfile.ZipFile(zip_name, mode='w')
zf.write(lambda_source_code, arcname=lambda_source_code.split('/')[-1])
zf.close()


S3Uploader.upload(local_path=zip_name, 
                  desired_s3_uri='s3://{}/{}'.format(bucket, project_name),
                  session=session)

lambda_client = boto3.client('lambda')

# delete original lambda before creating the new one
lambda_client.delete_function(FunctionName=function_name)
response = lambda_client.create_function(
    FunctionName=function_name,
    Runtime='python3.7',
    Role=lambda_role,
    Handler='query_training_status.lambda_handler',
    Code={
        'S3Bucket': bucket,
        'S3Key': '{}/{}'.format(project_name, zip_name)
    },
    Description='Queries a SageMaker training job and return the results.',
    Timeout=15,
    MemorySize=128
)


# Create the Lambda that updates the registry
registry_function_name = "ModelRegistryUpdater"
registry_zip_name = 'model_registry_lambda.zip'
registry_lambda_source_code = './code/update_model_registry.py'

registry_zf = zipfile.ZipFile(registry_zip_name, mode='w')
registry_zf.write(registry_lambda_source_code, arcname=registry_lambda_source_code.split('/')[-1])
registry_zf.close()


S3Uploader.upload(local_path=registry_zip_name, 
                  desired_s3_uri='s3://{}/{}'.format(bucket, project_name),
                  session=session)

lambda_client = boto3.client('lambda')

# delete original lambda before creating the new one
try:
    # deals with first run. If not there, just ignore the exception
    lambda_client.delete_function(FunctionName=registry_function_name)
except:
    pass

response = lambda_client.create_function(
    FunctionName=registry_function_name,
    Runtime='python3.7',
    Role=registry_lambda_role,
    Handler='update_model_registry.handler',
    Code={
        'S3Bucket': bucket,
        'S3Key': '{}/{}'.format(project_name, registry_zip_name)
    },
    Description='Updates the model registry DynamoDB table.',
    Timeout=15,
    MemorySize=128
)









# Create the estimator
container = get_image_uri(region, 'xgboost')

xgb = sagemaker.estimator.Estimator(container,
                                    sagemaker_execution_role, 
                                    train_instance_count=1, 
                                    train_instance_type='ml.m4.xlarge',
                                    output_path='s3://{}/{}/output'.format(bucket, project_name))

xgb.set_hyperparameters(max_depth=5,
                        eta=0.2,
                        gamma=4,
                        min_child_weight=6,
                        subsample=0.8,
                        silent=0,
                        objective='binary:logistic',
                        eval_metric='error',
                        num_round=100)


# Build out the workflow
execution_input = ExecutionInput(schema={
    'TrainingJobName': str,
    'GlueJobName': str,
    'ModelName': str,
    'EndpointName': str

})

etl_step = steps.GlueStartJobRunStep(
    'Extract, Transform, Load',
    parameters={"JobName": job_name,
                "Arguments":{
                    '--S3_SOURCE': data_source,
                    '--S3_DEST': 's3a://{}/{}/'.format(bucket, project_name),
                    '--TRAIN_KEY': train_prefix + '/',
                    '--VAL_KEY': val_prefix +'/'}
               }
)


training_step = steps.TrainingStep(
    'Model Training', 
    estimator=xgb,
    data={
        'train': s3_input(train_data, content_type='csv'),
        'validation': s3_input(validation_data, content_type='csv')
    },
    job_name=training_job_name,
    wait_for_completion=True
)

model_step = steps.ModelStep(
    'Save Model',
    model=training_step.get_expected_model(),
    model_name=execution_input['ModelName'],
    result_path='$.ModelStepResults'
)

lambda_step = steps.compute.LambdaStep(
    'Query Training Results',
    parameters={  
        "FunctionName": function_name,
        'Payload':{
            "TrainingJobName.$": "$.TrainingJobName"
        }
    }
)



check_accuracy_step = steps.states.Choice(
    'Accuracy > 90%'
)

arn_function_name = "arn:aws:lambda:eu-west-1:963778699255:function:" + registry_function_name
registry_lambda_step = steps.compute.LambdaStep(
    'Update Model Registry',
    parameters={  
        "FunctionName": arn_function_name,
        'Payload':{
            #"TrainingJobName.$": "$.TrainingJobName",
            'run_id' : project_name,  # get the step function version,
            'environment': "DEV",
            'algorithm': "xgboost",
            'model_location' : 's3://{}/{}/output'.format(bucket, project_name)
        }
    }
)


# This script does not do the deployment, this however illustrates how it could be done
'''endpoint_config_step = steps.EndpointConfigStep(
    "Create Model Endpoint Config",
    endpoint_config_name=execution_input['ModelName'],
    model_name=execution_input['ModelName'],
    initial_instance_count=1,
    instance_type='ml.m4.xlarge'
)

endpoint_step = steps.EndpointStep(
    'Update Model Endpoint',
    endpoint_name=execution_input['EndpointName'],
    endpoint_config_name=execution_input['ModelName'],
    update=False
)'''

fail_step = steps.states.Fail(
    'Model Accuracy Too Low',
    comment='Validation accuracy lower than threshold'
)

threshold_rule = steps.choice_rule.ChoiceRule.NumericLessThan(variable=lambda_step.output()['Payload']['trainingMetrics'][0]['Value'], value=.1)

check_accuracy_step.add_choice(rule=threshold_rule, next_step=registry_lambda_step)
check_accuracy_step.default_choice(next_step=fail_step)

#endpoint_config_step.next(endpoint_step)
#endpoint_config_step.next(registry_lambda_step)

workflow_definition = steps.Chain([
    etl_step,
    training_step,
    model_step,
    lambda_step,
    check_accuracy_step
])

# This can be used to create a brand new workflow
'''workflow = Workflow(
    name=workflow_name,
    definition=workflow_definition,
    role=workflow_execution_role,
    execution_input=execution_input
)'''


# This is used to update the existing workflow. 
# That way you can still see all the step function run history
# You could alternatively delete and recreate the workflow
workflow = Workflow.attach(state_machine_arn='arn:aws:states:eu-west-1:963778699255:stateMachine:MyInferenceRoutine_c020134fb5334562bb3c31e6d02cc77d')
workflow.update(
    definition = workflow_definition,
    role=workflow_execution_role
)

# Finally, run the workflow!
'''execution = workflow.execute(
    inputs={
        'TrainingJobName': 'regression-{}'.format(id), # Each Sagemaker Job requires a unique name,
        'ModelName': 'CustomerChurn-{}'.format(id), # Each Model requires a unique name,
        'EndpointName': 'CustomerChurn', # Each Endpoint requires a unique name
    }
)'''