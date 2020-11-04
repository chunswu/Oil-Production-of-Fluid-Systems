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

    df.printSchema()
    print(df.count())

    df.createOrReplaceTempView("data")

    niobrara_df = spark.sql("""
                        SELECT 
                            api,
                            state,
                            UPPER(formation) AS formation, 
                            Prod180DayOil AS day180,
                            Prod365DayOil AS day365,
                            Prod545DayOil AS day545,
                            Prod730DayOil AS day730,
                            Prod1095DayOil AS day1095,
                            Prod1460DayOil AS day1460,
                            Prod1825DayOil AS day1825,
                            TotalProppant
                        FROM data
                        WHERE state = "COLORADO" AND formation = 'NIOBRARA'
                        ORDER BY day180 DESC
                        """)

    niobrara_df = drop_na_column(niobrara_df, ["TotalProppant"])
    niobrara_df.show()
    col_names = niobrara_df.schema.names

    col_names = niobrara_df.schema.names

    day180_df = drop_na_column(niobrara_df,col_names[3])
    y_axis = list()
    day = day180_df.rdd.map(lambda y: y.day180).collect()
    well_180 = np.median(day)
    y_axis.append(well_180)

    day365_df = drop_na_column(niobrara_df, col_names[3:5])
    day = day365_df.rdd.map(lambda y: y.day365).collect()
    well_365 = np.median(day)
    y_axis.append(well_365)

    day545_df = drop_na_column(niobrara_df, col_names[3:6])

    day = day545_df.rdd.map(lambda y: y.day545).collect()
    well_545 = np.median(day)
    y_axis.append(well_545)

    day730_df = drop_na_column(niobrara_df, col_names[3:7])
    day = day730_df.rdd.map(lambda y: y.day730).collect()
    well_730 = np.median(day)
    y_axis.append(well_730)

    day1095_df = drop_na_column(niobrara_df, col_names[3:8])
    day = day1095_df.rdd.map(lambda y: y.day1095).collect()
    well_1095 = np.median(day)
    y_axis.append(well_1095)

    day1460_df = drop_na_column(niobrara_df, col_names[3:9])
    day = day1460_df.rdd.map(lambda y: y.day1460).collect()
    well_1460 = np.median(day)
    y_axis.append(well_1460)

    day1825_df = drop_na_column(niobrara_df, col_names[3:10])
    day = day1825_df.rdd.map(lambda y: y.day1825).collect()
    well_1825 = np.median(day)
    y_axis.append(well_1825)

    codell_df = spark.sql("""
                        SELECT 
                            api,
                            state,
                            UPPER(formation) AS formation, 
                            Prod180DayOil AS day180,
                            Prod365DayOil AS day365,
                            Prod545DayOil AS day545,
                            Prod730DayOil AS day730,
                            Prod1095DayOil AS day1095,
                            Prod1460DayOil AS day1460,
                            Prod1825DayOil AS day1825,
                            TotalProppant
                        FROM data
                        WHERE state = "COLORADO" AND formation = 'CODELL'
                        ORDER BY day180 DESC
                        """)

    codell_df = drop_na_column(codell_df, ["TotalProppant"])
    codell_df.show()

    day180_df = drop_na_column(codell_df, col_names[3])
    y_axis_c = list()
    day = day180_df.rdd.map(lambda y: y.day180).collect()
    well_180 = np.median(day)
    y_axis_c.append(well_180)

    day365_df = drop_na_column(codell_df, col_names[3:5])
    day = day365_df.rdd.map(lambda y: y.day365).collect()
    well_365 = np.median(day)
    y_axis_c.append(well_365)

    day545_df = drop_na_column(codell_df, col_names[3:6])
    day = day545_df.rdd.map(lambda y: y.day545).collect()
    well_545 = np.median(day)
    y_axis_c.append(well_545)

    day730_df = drop_na_column(codell_df, col_names[3:7])
    day = day730_df.rdd.map(lambda y: y.day730).collect()
    well_730 = np.median(day)
    y_axis_c.append(well_730)

    day1095_df = drop_na_column(codell_df, col_names[3:8])
    day = day1095_df.rdd.map(lambda y: y.day1095).collect()
    well_1095 = np.median(day)
    y_axis_c.append(well_1095)

    day1460_df = drop_na_column(codell_df, col_names[3:9])
    day = day1460_df.rdd.map(lambda y: y.day1460).collect()
    well_1460 = np.median(day)
    y_axis_c.append(well_1460)


    day1825_df = drop_na_column(codell_df, col_names[3:10])
    day = day1825_df.rdd.map(lambda y: y.day1825).collect()
    well_1825 = np.median(day)
    y_axis_c.append(well_1825)

    nio_prop = niobrara_df.rdd.map(lambda x: x.TotalProppant).collect()
    cod_prop = codell_df.rdd.map(lambda x: x.TotalProppant).collect()

    nio_prop = np.array(nio_prop)/10**6
    cod_prop = np.array(cod_prop)/10**6

    x_labels = ['Niobrara', 'Codell']

    fig, ax = plt.subplots(figsize = (10, 10))
    ax.boxplot([nio_prop, cod_prop], 
                showfliers=False, 
                medianprops=dict(color="orange", lw=5), 
                boxprops=dict(color='navy', lw=3),
                capprops=dict(color='navy', lw=3),
                whiskerprops=dict(color='navy', lw=3))

    ax.set_xticklabels(x_labels)
    ax.set_title('Proppant Used By Formation', fontsize=34)
    ax.set_xlabel('Formations', fontsize=24)
    ax.tick_params(axis='both', which='major', labelsize=18)
    ax.set_ylabel('Proppant (Millions of Pounds)', fontsize=24)
    plt.savefig('../images/formation_proppant.png')


    fig, ax = plt.subplots(figsize = (12, 6))
    x_axis = [180, 365, 545, 730, 1095, 1460, 1825]
    line_plt(ax, x_axis, y_axis, 'red', 'Niobrara')
    line_plt(ax, x_axis, y_axis_c, 'blue', 'Codell')
    ax.set_title('Oil Production', fontsize=34)
    ax.set_xlabel('Days', fontsize=24)
    ax.tick_params(axis='both', which='major', labelsize=16)
    ax.set_ylabel('Cummulative Barrels', fontsize=24)
    ax.legend(loc='lower right', fontsize=21)
    ax.set_ylim(0)
    plt.savefig('../images/production_formation.png')

    print("EOF")
