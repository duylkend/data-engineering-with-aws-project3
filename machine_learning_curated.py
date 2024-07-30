import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722355137975 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1722355137975")

# Script generated for node Trainer Trusted
TrainerTrusted_node1722355138138 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/step_trainer/trusted/"], "recurse": True}, transformation_ctx="TrainerTrusted_node1722355138138")

# Script generated for node Join
Join_node1722355206067 = Join.apply(frame1=TrainerTrusted_node1722355138138, frame2=AccelerometerTrusted_node1722355137975, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1722355206067")

# Script generated for node Amazon S3
AmazonS3_node1722355490626 = glueContext.getSink(path="s3://duylk16/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722355490626")
AmazonS3_node1722355490626.setCatalogInfo(catalogDatabase="database-gl",catalogTableName="machine_learning_curated")
AmazonS3_node1722355490626.setFormat("json")
AmazonS3_node1722355490626.writeFrame(Join_node1722355206067)
job.commit()