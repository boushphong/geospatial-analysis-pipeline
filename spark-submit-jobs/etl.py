from sedona.register import SedonaRegistrator
from sedona.utils import KryoSerializer, SedonaKryoRegistrator
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, expr, to_date, col
import geopandas as gpd
import pyproj
import shapely.geometry as shpgeo


spark = SparkSession.builder.appName("Ansarada") \
    .config("spark.serializer", KryoSerializer.getName) \
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
    .config("spark.driver.memory", "8G") \
    .config("spark.executor.memory", "16G") \
    .config("spark.jars", """jars/geotools-wrapper-1.1.0-25.2.jar,
                             jars/sedona-python-adapter-3.0_2.12-1.2.0-incubating.jar,
                             jars/sedona-viz-3.0_2.12-1.2.0-incubating.jar""").getOrCreate()

SedonaRegistrator.registerAll(spark)


# Buffer JFK Polygon function
def toFromUTM(shp, proj, inv=False):
    geoInterface = shp.__geo_interface__

    shpType = geoInterface["type"]
    coords = geoInterface["coordinates"]
    if shpType == "Polygon":
        newCoord = [[proj(*point, inverse=inv) for point in linring] for linring in coords]
    elif shpType == "MultiPolygon":
        newCoord = [[[proj(*point, inverse=inv) for point in linring] for linring in poly] for poly in coords]

    return shpgeo.shape({"type": shpType, "coordinates": tuple(newCoord)})


if __name__ == "__main__":
    # Getting Buffer JFK Airport Polygon Object 
    NY_airport = gpd.read_file("data/Airport Polygon.geojson")

    jfk_airport_multipolygon = NY_airport["geometry"][1]

    proj = pyproj.Proj(proj="utm", zone=18, ellps="WGS84", datum="WGS84")

    init_shape_utm = toFromUTM(jfk_airport_multipolygon, proj)

    buffer_shape_utm = init_shape_utm.buffer(500)

    buffer_shape_lonlat = toFromUTM(buffer_shape_utm, proj, inv=True)

    # Filtering Invalid 
    df = spark.read.option("header", True) \
        .option("inferSchema", True) \
        .csv("data/yellow_tripdata_2009-12.csv") \
        .drop_duplicates()

    df = df.filter((abs("End_Lat") <= 90) &
                   (abs("Start_Lat") <= 90) &
                   (abs("End_Lon") <= 180) &
                   (abs("Start_Lon") <= 180))

    df = df.filter(((col("Start_Lat") != col("End_Lat")) & (col("Start_Lon") != col("End_Lon"))))

    df = df.withColumn('Coordinate', expr("ST_Point(End_Lon, End_Lat)"))

    df.cache()
    df.count()

    df_final = df.withColumn("To_JFK_Airport",
                             expr(f"ST_Intersects(ST_GeomFromText('{str(buffer_shape_lonlat)}'), Coordinate)"))

    df_final = df_final.repartition(48)

    df_final.withColumn("PARTITION", to_date(col("Trip_Pickup_Datetime"))) \
    	.drop("Coordinate") \
    	.write \
        .partitionBy("PARTITION") \
        .mode("overwrite") \
        .parquet("output/taxi_data")
