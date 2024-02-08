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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707001542539 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1707001542539",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1707433741154 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1707433741154",
)

# Script generated for node Join Accelerometer
SqlQuery0 = """
select * from cus_trusted
where email in
(select distinct user from acc_trusted)
"""
JoinAccelerometer_node1707002359069 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "acc_trusted": AccelerometerTrusted_node1707001542539,
        "cus_trusted": CustomerTrusted_node1707433741154,
    },
    transformation_ctx="JoinAccelerometer_node1707002359069",
)

# Script generated for node Remove PII
RemovePII_node1707433765419 = DropFields.apply(
    frame=JoinAccelerometer_node1707002359069,
    paths=["customerName", "email", "phone"],
    transformation_ctx="RemovePII_node1707433765419",
)

# Script generated for node Customer Curated
CustomerCurated_node1707002853147 = glueContext.getSink(
    path="s3://kostiantyn-lobanov-aws-bucket/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1707002853147",
)
CustomerCurated_node1707002853147.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1707002853147.setFormat("json")
CustomerCurated_node1707002853147.writeFrame(RemovePII_node1707433765419)
job.commit()
