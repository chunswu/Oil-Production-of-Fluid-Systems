from capstone import *

if __name__ == '__main__':

    spark = (ps.sql.SparkSession.builder 
        .master("local[4]") 
        .appName("sparkSQL exercise") 
        .getOrCreate()
        )
    sc = spark.sparkContext

    df = spark.read.csv('../data/dj_basin.csv',
                         header=True,
                         quote='"',
                         sep=",",
                         inferSchema=True)

    df.createOrReplaceTempView("data")

    coorid_df = spark.sql("""
                        SELECT 
                            api,
                            State,
                            Latitude, 
                            Longitude
                        FROM data
                        WHERE State = 'COLORADO'
                        """)

    coorid_df = coorid_df.na.replace({104.8865041: -104.8865041})
    coorid_df = drop_na_column(coorid_df, ["Latitude", "Longitude"])
    latitude = coorid_df.rdd.map(lambda y: y.Latitude).collect()
    longitude = coorid_df.rdd.map(lambda x: x.Longitude).collect()
    x_lat = np.array(latitude)
    y_long = np.array(longitude)
    print(x_lat)
    bbox = ((min(y_long), max(y_long),      
            min(x_lat), max(x_lat)))
    map_ = plt.imread('../images/map_clear.png')

    fig, ax = plt.subplots(figsize = (20, 25))
    scatter_plt(ax, y_long, x_lat, 'Colorado Well Locations', 'Latitude', 'Longitude', 'dodgerblue', zorder=1)
    ax.tick_params(axis='both', which='major', labelsize=16)
    ax.set_xlim(bbox[0], bbox[1])
    ax.set_ylim(bbox[2], bbox[3])
    ax.imshow(map_, zorder=0, extent = bbox, aspect='auto')

    plt.savefig('../images/well_location.png')

    print("EOF")
