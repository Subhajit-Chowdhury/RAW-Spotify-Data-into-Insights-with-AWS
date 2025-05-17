import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Artists
Artists_node1747331724579 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://myspot-project/staging/artists.csv"], "recurse": True}, transformation_ctx="Artists_node1747331724579")

# Script generated for node Tracks
Tracks_node1747331872214 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://myspot-project/staging/tracks.csv"], "recurse": True}, transformation_ctx="Tracks_node1747331872214")

# Script generated for node Albums
Albums_node1747331827418 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://myspot-project/staging/albums.csv"], "recurse": True}, transformation_ctx="Albums_node1747331827418")

# Script generated for node Joining Albums with Artists
JoiningAlbumswithArtists_node1747331916715 = Join.apply(frame1=Albums_node1747331827418, frame2=Artists_node1747331724579, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoiningAlbumswithArtists_node1747331916715")

# Script generated for node Joining with Tracks
JoiningwithTracks_node1747332420338 = Join.apply(frame1=Tracks_node1747331872214, frame2=JoiningAlbumswithArtists_node1747331916715, keys1=["track_id"], keys2=["track_id"], transformation_ctx="JoiningwithTracks_node1747332420338")

# Script generated for node Drop Fields
DropFields_node1747332580485 = DropFields.apply(frame=JoiningwithTracks_node1747332420338, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1747332580485")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1747332580485, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747331714761", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1747332619945 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1747332580485, connection_type="s3", format="glueparquet", connection_options={"path": "s3://myspot-project/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1747332619945")

job.commit()