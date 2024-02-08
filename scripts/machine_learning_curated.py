import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Curated
AccelerometerCurated_node1707258120167 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kostiantyn-lobanov-aws-bucket/accelerometer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerCurated_node1707258120167",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707340270013 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kostiantyn-lobanov-aws-bucket/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1707340270013",
)

# Script generated for node Join
AccelerometerCurated_node1707258120167DF = AccelerometerCurated_node1707258120167.toDF()
StepTrainerTrusted_node1707340270013DF = StepTrainerTrusted_node1707340270013.toDF()
Join_node1707344365901 = DynamicFrame.fromDF(
    AccelerometerCurated_node1707258120167DF.join(
        StepTrainerTrusted_node1707340270013DF,
        (
            AccelerometerCurated_node1707258120167DF["timestamp"]
            == StepTrainerTrusted_node1707340270013DF["sensorreadingtime"]
        ),
        "outer",
    ),
    glueContext,
    "Join_node1707344365901",
)

# Script generated for node Drop Fields
DropFields_node1707434037553 = DropFields.apply(
    frame=Join_node1707344365901,
    paths=["user", "sensorreadingtime"],
    transformation_ctx="DropFields_node1707434037553",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1707258535734 = glueContext.getSink(
    path="s3://kostiantyn-lobanov-aws-bucket/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1707258535734",
)
MachineLearningCurated_node1707258535734.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1707258535734.setFormat("json")
MachineLearningCurated_node1707258535734.writeFrame(DropFields_node1707434037553)
job.commit()
