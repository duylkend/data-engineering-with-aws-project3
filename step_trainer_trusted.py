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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1722348737554 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1722348737554")

# Script generated for node Customers Curated
CustomersCurated_node1722348737381 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/customer/curated/"], "recurse": True}, transformation_ctx="CustomersCurated_node1722348737381")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1722354193315 = ApplyMapping.apply(frame=StepTrainerLanding_node1722348737554, mappings=[("sensorreadingtime", "long", "sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1722354193315")

# Script generated for node Join
Join_node1722351196131 = Join.apply(frame1=RenamedkeysforJoin_node1722354193315, frame2=CustomersCurated_node1722348737381, keys1=["right_serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1722351196131")

# Script generated for node Drop Fields
DropFields_node1722353190761 = DropFields.apply(frame=Join_node1722351196131, paths=["registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "lastupdatedate", "email", "serialnumber", "phone", "sharewithresearchasofdate"], transformation_ctx="DropFields_node1722353190761")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1722352206546 = glueContext.getSink(path="s3://duylk16/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1722352206546")
StepTrainerTrusted_node1722352206546.setCatalogInfo(catalogDatabase="database-gl",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1722352206546.setFormat("json")
StepTrainerTrusted_node1722352206546.writeFrame(DropFields_node1722353190761)
job.commit()