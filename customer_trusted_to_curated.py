import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1722347675459 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1722347675459")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722347675763 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1722347675763")

# Script generated for node Join
Join_node1722347825825 = Join.apply(frame1=AccelerometerTrusted_node1722347675763, frame2=CustomerTrusted_node1722347675459, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1722347825825")

# Script generated for node Select Fields
SelectFields_node1722347866782 = SelectFields.apply(frame=Join_node1722347825825, paths=["email", "phone"], transformation_ctx="SelectFields_node1722347866782")

# Script generated for node Drop Duplicates
DropDuplicates_node1722348009240 =  DynamicFrame.fromDF(SelectFields_node1722347866782.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1722348009240")

# Script generated for node Customer Curated
CustomerCurated_node1722348072750 = glueContext.getSink(path="s3://duylk16/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1722348072750")
CustomerCurated_node1722348072750.setCatalogInfo(catalogDatabase="database-gl",catalogTableName="customers_curated")
CustomerCurated_node1722348072750.setFormat("json")
CustomerCurated_node1722348072750.writeFrame(DropDuplicates_node1722348009240)
job.commit()