import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1707080599858 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1707080599858",
)

# Script generated for node Customer Curated
CustomerCurated_node1707255869045 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kostiantyn-lobanov-aws-bucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1707255869045",
)

# Script generated for node Filter Step Trainer
SqlQuery0 = """
select * from st
where serialnumber in (select distinct serialnumber from cc)
"""
FilterStepTrainer_node1707080693659 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "st": StepTrainerLanding_node1707080599858,
        "cc": CustomerCurated_node1707255869045,
    },
    transformation_ctx="FilterStepTrainer_node1707080693659",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707081211359 = glueContext.write_dynamic_frame.from_options(
    frame=FilterStepTrainer_node1707080693659,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kostiantyn-lobanov-aws-bucket/step_trainer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1707081211359",
)

job.commit()
