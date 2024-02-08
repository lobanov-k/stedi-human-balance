import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1706739850360 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1706739850360",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1706739843655 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1706739843655",
)

# Script generated for node Join Customer
JoinCustomer_node1706740390498 = Join.apply(
    frame1=AccelerometerLanding_node1706739843655,
    frame2=CustomerTrusted_node1706739850360,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1706740390498",
)

# Script generated for node Drop Fields
DropFields_node1706740886094 = DropFields.apply(
    frame=JoinCustomer_node1706740390498,
    paths=[
        "email",
        "phone",
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "sharewithresearchasofdate",
        "customername",
        "sharewithfriendsasofdate",
        "lastupdatedate",
    ],
    transformation_ctx="DropFields_node1706740886094",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1706740430620 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706740886094,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kostiantyn-lobanov-aws-bucket/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrustedZone_node1706740430620",
)

job.commit()
