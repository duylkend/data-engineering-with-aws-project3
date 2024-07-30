import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1722344907011 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://duylk16/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1722344907011")

# Script generated for node Change Schema
ChangeSchema_node1722344946897 = ApplyMapping.apply(frame=CustomerLanding_node1722344907011, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "long", "registrationdate", "long"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long")], transformation_ctx="ChangeSchema_node1722344946897")

# Script generated for node Filter
Filter_node1722345807562 = Filter.apply(frame=ChangeSchema_node1722344946897, f=lambda row: (row["sharewithresearchasofdate"] > 0), transformation_ctx="Filter_node1722345807562")

# Script generated for node Customer Trusted
CustomerTrusted_node1722344997346 = glueContext.getSink(path="s3://duylk16/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1722344997346")
CustomerTrusted_node1722344997346.setCatalogInfo(catalogDatabase="database-gl",catalogTableName="customer_trusted")
CustomerTrusted_node1722344997346.setFormat("json")
CustomerTrusted_node1722344997346.writeFrame(Filter_node1722345807562)
job.commit()