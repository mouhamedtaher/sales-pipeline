"""Point d'entrée exécuté directement par AWS Glue (Script location).

Ce fichier n'est PAS un module du package sales_pipeline : c'est le script
"driver" que Glue lance. Il se contente de démarrer les contextes
Glue/Spark, lire les arguments du job, et déléguer tout le travail à
SalesPipelineJob. Le package sales_pipeline doit être fourni séparément
via le paramètre Glue "--extra-py-files" (sales_pipeline.zip).
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from sales_pipeline.job import SalesPipelineJob

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

try:
    pipeline = SalesPipelineJob(
        spark=spark,
        input_path=args["input_path"],
        output_path=args["output_path"],
    )
    pipeline.run()
finally:
    job.commit()
