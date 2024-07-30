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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1722346337072 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1722346337072")

# Script generated for node Customer Trusted
CustomerTrusted_node1722346343005 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1722346343005")

# Script generated for node Join
Join_node1722346422275 = Join.apply(frame1=AccelerometerLanding_node1722346337072, frame2=CustomerTrusted_node1722346343005, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1722346422275")

# Script generated for node Select Fields
SelectFields_node1722346725630 = SelectFields.apply(frame=Join_node1722346422275, paths=["z", "y", "x", "user", "timestamp"], transformation_ctx="SelectFields_node1722346725630")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722346795168 = glueContext.getSink(path="s3://duylk16/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1722346795168")
AccelerometerTrusted_node1722346795168.setCatalogInfo(catalogDatabase="database-gl",catalogTableName=" accelerometer_trusted")
AccelerometerTrusted_node1722346795168.setFormat("json")
AccelerometerTrusted_node1722346795168.writeFrame(SelectFields_node1722346725630)
job.commit()