import pyspark as ps
from pyspark.sql.types import *
from pyspark.sql.functions import struct, col, when, lit
import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 16, 'font.family': 'sans'})
import numpy as np
import scipy.stats as stats

plt.style.use('ggplot')


def scatter_plt(ax, x, y, title, xlab, ylab, color, zorder=1):
    '''Create a scatter plot

    Parameters
    ----------
    ax: plot axis
    x: list in the x-axis
    y: list in the y-axis
    title: str
    xlab: str
    ylab: str
    color: str
    zorder: int, default set to 1

    Returns
    -------
    None
    '''
    ax.scatter(x, y, alpha= 0.5, color=color, s=50, zorder=1)
    ax.set_title(title, fontsize=35)
    ax.set_ylabel(xlab, fontsize=20)
    ax.set_xlabel(ylab, fontsize=20)

def line_plt(ax, x, y, color, label):
    '''Create a line plot

    Parameters
    ----------
    ax: plot axis
    x: list in the x-axis
    y: list in the y-axis
    label: str
    color: str

    Returns
    -------
    None
    '''
    ax.plot(x, y, linewidth=2, color=color, label=label)

    def drop_na_column(df, lst):
    '''Removes rows with null or n/a values from a dataframe

    Parameters
    ----------
    df: dataframe in sparks
    lst: list of strings
    
    Returns
    -------
    returns a dataframe
    '''
    return df.na.drop(subset=lst)

def bar_plot(ax, data, label):
    '''Create a bar plot

    Parameters
    ----------
    ax: plot axis
    data: list of ints
    label: string
    Returns
    -------
    None
    '''
    ax.bar(label, data, label=label)

def fix_fluid_types(df, lst1, lst2):
    '''Renames the fluid types in a dataframe with the correct fluid type

    Parameters
    ----------
    df: dataframe in sparks
    lst1: list of strings
    lst2: list of strings
    
    Returns
    -------
    returns a dataframe
    '''    
    
    
    for i in range(1,6):
        df = df.na.replace(wrong_fluid, right_fluid, 'fluid_type'+str(i))
    return df

def fill_fluid_na(df):
    '''Replaces null or na in a column wiht blank or 0

    Parameters
    ----------
    df: dataframe in sparks
    
    Returns
    -------
    returns a dataframe 
    ''' 
    for i in range(1,6):
        df = df.na.fill({'fluid_type'+str(i): ''})
        df = df.na.fill({'FluidVol'+str(i): 0})
    
    return df

def clean_fluid_type(df, fluid_sys):
    '''Passed in dataframe and strings of fluid systems and sums up the volumes
       across all the fluid types

    Parameters
    ----------
    df: dataframe
    fluid_sys: string name of fluid system
    
    Returns
    -------
    returns df
    '''
    
    fluid_vol = 'FluidVol'
    fluid_type = 'fluid_type'
    lowcase_fluid = fluid_sys.lower() + "_collect"
    
    df = df.withColumn(lowcase_fluid, lit(0))
    for i in range(1, 6):
        df = df.withColumn(fluid_sys.lower()+str(i), when(col(fluid_type+str(i)) == fluid_sys, col(fluid_vol+str(i))).otherwise(0))
        df = df.withColumn(lowcase_fluid, col(lowcase_fluid) + col(fluid_sys.lower()+str(i)))

    return df

def winner_counter(arr1, arr2):
    '''Compares 2 arrays and keeps count of a win

    Parameters
    ----------
    arr1: np.array
    arr2: np.array
    
    Returns
    -------
    returns int of sum of wins
    ''' 
    arr1, arr2 = np.array(arr1), np.array(arr2)
    win_total = 0
    for x in arr1:
        small_win = np.sum(x > arr2) + 0.5*np.sum(x == arr2)
        win_total += small_win
    return win_total


if __name__ == '__main__':

    spark = (ps.sql.SparkSession.builder 
        .master("local[4]") 
        .appName("sparkSQL exercise") 
        .getOrCreate()
        )
    sc = spark.sparkContext

    df = spark.read.csv('./data/dj_basin.csv',
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
    plt.savefig('./images/formation_proppant.png')


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
    plt.savefig('images/production_formation.png')


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
    bbox = ((min(y_long), max(y_long),      
            min(x_lat), max(x_lat)))
    map_ = plt.imread('./images/map_clear.png')

    fig, ax = plt.subplots(figsize = (20, 25))
    scatter_plt(ax, y_long, x_lat, 'Colorado Well Locations', 'Latitude', 'Longitude', 'dodgerblue', zorder=1)
    ax.tick_params(axis='both', which='major', labelsize=16)
    ax.set_xlim(bbox[0], bbox[1])
    ax.set_ylim(bbox[2], bbox[3])
    ax.imshow(map_, zorder=0, extent = bbox, aspect='auto')

    plt.savefig('./images/well_location.png')

    fluid_df = spark.sql("""
                    SELECT
                        api,
                        State,
                        TotalCleanVol,
                        FluidVol1,
                        UPPER(FluidType1) AS fluid_type1,
                        FluidVol2,
                        UPPER(FluidType2) AS fluid_type2,
                        FluidVol3,
                        UPPER(FluidType3) AS fluid_type3,
                        FluidVol4,
                        UPPER(FluidType4) AS fluid_type4,
                        FluidVol5,
                        UPPER(FluidType5) AS fluid_type5
                    FROM data
                    WHERE State = 'COLORADO'
                    """)

    fluid_df = drop_na_column(fluid_df, ["fluid_type1"])
    wrong_fluid = ['HYBRID|X-LINK', 'X-LINK', 'ACID|OTHER FLUID', 'OTHER FLUID|WATER', 
                'HYBRID|LINEAR GEL', 'HYBRID|SLICKWATER', 'X-LINK|SLICKWATER', 'ACID|X-LINK', 'GEL|LINEAR GEL']
    right_fluid = ['HYBRID', 'GEL', 'ACID', 'WATER', 'HYBRID', 'HYBRID', 'HYBRID', 'HYBRID', 'GEL']

    fluid_df = fix_fluid_types(fluid_df, wrong_fluid, right_fluid)
    fluid_df = fill_fluid_na(fluid_df)
        
    fluid_df = drop_na_column(fluid_df, ["TotalCleanVol"])
    fluid_df = fluid_df.distinct()

    fluid_df = clean_fluid_type(fluid_df, 'HYBRID')
    # fluid_df.show()
    fluid_df = clean_fluid_type(fluid_df, 'SLICKWATER')
    # fluid_df.show()
    fluid_df = clean_fluid_type(fluid_df, 'GEL')
    # fluid_df.show()

    columns_to_drop = ['hybrid1', 'hybrid2', 'hybrid3', 'hybrid4', 'hybrid5',
                    'slickwater1', 'slickwater2', 'slickwater3', 'slickwater4', 'slickwater5',
                    'gel1', 'gel2', 'gel3', 'gel4', 'gel5',
                    'FluidVol1', 'fluid_type1','FluidVol2','fluid_type2', 'FluidVol3', 
                    'fluid_type3', 'FluidVol4', 'fluid_type4', 'FluidVol5', 'fluid_type5']
    fluid_df = fluid_df.drop(*columns_to_drop)

    production_df = spark.sql("""
                    SELECT 
                        api,
                        State,
                        UPPER(formation) AS formation, 
                        Prod545DayOil AS day545
                    FROM data
                    WHERE state = "COLORADO"
                    """)

    compare_df = fluid_df.join(production_df, ['api'], 'left_outer')
    compare_df = drop_na_column(compare_df, ['day545'])
    columns_to_drop = ['State', 'TotalCleanVol']
    compare_df = compare_df.drop(*columns_to_drop)
    compare_df.show()

    compare_df.createOrReplaceTempView("design_data")
    slick_production_df = spark.sql("""
                        SELECT
                            api,
                            slickwater_collect,
                            day545
                        FROM design_data
                        WHERE slickwater_collect > 0
                        ORDER BY day545 DESC
                        """)
    print(slick_production_df.count())

    gel_production_df = spark.sql("""
                        SELECT
                            api,
                            gel_collect,
                            day545
                        FROM design_data
                        WHERE gel_collect > 0
                        ORDER BY day545 DESC
                        """)
    print(gel_production_df.count())

    hybrid_production_df = spark.sql("""
                        SELECT
                            api,
                            hybrid_collect,
                            day545
                        FROM design_data
                        WHERE hybrid_collect > 0
                        ORDER BY day545 DESC
                        """)
    print(hybrid_production_df.count())

    slick_production = slick_production_df.rdd.map(lambda x: x.day545).collect()
    gel_production = gel_production_df.rdd.map(lambda x: x.day545).collect()
    hybrid_production = hybrid_production_df.rdd.map(lambda x: x.day545).collect()

    fig, ax = plt.subplots(figsize = (24, 6))
    ax.hist(np.log(slick_production), bins=20, label='oil produced')

    fig, ax = plt.subplots(figsize = (24, 6))
    ax.hist(np.log(gel_production), bins=20, label='oil produced')

    fig, ax = plt.subplots(figsize = (24, 6))
    ax.hist(np.log(hybrid_production), bins=20, label='oil produced')

    slick_wins = winner_counter(slick_production, gel_production)
    gel_wins = winner_counter(gel_production, slick_production)
    print("Number of Slick Wins: {}".format(slick_wins))
    print("Number of Gel Wins: {}".format(gel_wins))
    res = stats.mannwhitneyu(slick_production, gel_production, alternative="two-sided")
    print("p-value for Slick = Gel: {}".format(res.pvalue))

    slick_wins = winner_counter(slick_production, hybrid_production)
    hybrid_wins = winner_counter(hybrid_production, slick_production)
    print("Number of Slick Wins: {}".format(slick_wins))
    print("Number of hybrid Wins: {}".format(hybrid_wins))
    res = stats.mannwhitneyu(slick_production, hybrid_production, alternative="two-sided")
    print("p-value for Slick = Hybrid: {}".format(res.pvalue))

    gel_wins = winner_counter(gel_production, hybrid_production)
    hybrid_wins = winner_counter(hybrid_production, gel_production)
    print("Number of Gel Wins: {}".format(gel_wins))
    print("Number of Hybrid Wins: {}".format(hybrid_wins))
    res = stats.mannwhitneyu(gel_production, hybrid_production, alternative="two-sided")
    print("p-value for Gel = Hybrid: {}".format(res.pvalue))

    alpha = .01
    hypo_combine = 3
    alpha_b = alpha/hypo_combine
    alpha_b

    slick_prod_nio_df = spark.sql("""
                    SELECT
                        api,
                        slickwater_collect,
                        day545
                    FROM design_data
                    WHERE slickwater_collect > 0 AND formation = "NIOBRARA"
                    ORDER BY day545 DESC
                    """)

    gel_prod_nio_df = spark.sql("""
                    SELECT
                        api,
                        slickwater_collect,
                        day545
                    FROM design_data
                    WHERE gel_collect > 0 AND formation = "NIOBRARA"
                    ORDER BY day545 DESC
                    """)

    hybrid_prod_nio_df = spark.sql("""
                    SELECT
                        api,
                        slickwater_collect,
                        day545
                    FROM design_data
                    WHERE hybrid_collect > 0 AND formation = "NIOBRARA"
                    ORDER BY day545 DESC
                    """)

    slick_prod_nio = slick_prod_nio_df.rdd.map(lambda x: x.day545).collect()
    gel_prod_nio = gel_prod_nio_df.rdd.map(lambda x: x.day545).collect()
    hybrid_prod_nio = hybrid_prod_nio_df.rdd.map(lambda x: x.day545).collect()

    slick_wins = winner_counter(slick_prod_nio, gel_prod_nio)
    gel_wins = winner_counter(gel_prod_nio, slick_prod_nio)
    print("Number of Slick Wins: {}".format(slick_wins))
    print("Number of Gel Wins: {}".format(gel_wins))
    res = stats.mannwhitneyu(slick_prod_nio, gel_prod_nio, alternative="two-sided")
    print("p-value for Slick = Gel: {}".format(res.pvalue))


    slick_wins = winner_counter(slick_prod_nio, hybrid_prod_nio)
    hybrid_wins = winner_counter(hybrid_prod_nio, slick_prod_nio)
    print("Number of Slick Wins: {}".format(slick_wins))
    print("Number of hybrid Wins: {}".format(hybrid_wins))
    res = stats.mannwhitneyu(slick_prod_nio, hybrid_prod_nio, alternative="two-sided")
    print("p-value for Slick = Hybrid: {}".format(res.pvalue))

    gel_wins = winner_counter(gel_prod_nio, hybrid_prod_nio)
    hybrid_wins = winner_counter(hybrid_prod_nio, gel_prod_nio)
    print("Number of Gel Wins: {}".format(gel_wins))
    print("Number of Hybrid Wins: {}".format(hybrid_wins))
    res = stats.mannwhitneyu(gel_prod_nio, hybrid_prod_nio, alternative="two-sided")
    print("p-value for Gel = Hybrid: {}".format(res.pvalue))


    slick_prod_cod_df = spark.sql("""
                        SELECT
                            api,
                            slickwater_collect,
                            day545
                        FROM design_data
                        WHERE slickwater_collect > 0 AND formation = "CODELL"
                        ORDER BY day545 DESC
                        """)

    gel_prod_cod_df = spark.sql("""
                        SELECT
                            api,
                            slickwater_collect,
                            day545
                        FROM design_data
                        WHERE gel_collect > 0 AND formation = "CODELL"
                        ORDER BY day545 DESC
                        """)

    hybrid_prod_cod_df = spark.sql("""
                        SELECT
                            api,
                            slickwater_collect,
                            day545
                        FROM design_data
                        WHERE hybrid_collect > 0 AND formation = "CODELL"
                        ORDER BY day545 DESC
                        """)

    slick_prod_cod = slick_prod_cod_df.rdd.map(lambda x: x.day545).collect()
    gel_prod_cod = gel_prod_cod_df.rdd.map(lambda x: x.day545).collect()
    hybrid_prod_cod = hybrid_prod_cod_df.rdd.map(lambda x: x.day545).collect()

    slick_wins = winner_counter(slick_prod_cod, gel_prod_cod)
    gel_wins = winner_counter(gel_prod_cod, slick_prod_cod)
    print("Number of Slick Wins: {}".format(slick_wins))
    print("Number of Gel Wins: {}".format(gel_wins))
    res = stats.mannwhitneyu(slick_prod_cod, gel_prod_cod, alternative="two-sided")
    print("p-value for Slick = Gel: {}".format(res.pvalue))

    slick_wins = winner_counter(slick_prod_cod, hybrid_prod_cod)
    hybrid_wins = winner_counter(hybrid_prod_cod, slick_prod_cod)
    print("Number of Slick Wins: {}".format(slick_wins))
    print("Number of hybrid Wins: {}".format(hybrid_wins))
    res = stats.mannwhitneyu(slick_prod_cod, hybrid_prod_cod, alternative="two-sided")
    print("p-value for Slick = Hybrid: {}".format(res.pvalue))

    gel_wins = winner_counter(gel_prod_cod, hybrid_prod_cod)
    hybrid_wins = winner_counter(hybrid_prod_cod, gel_prod_cod)
    print("Number of Gel Wins: {}".format(gel_wins))
    print("Number of Hybrid Wins: {}".format(hybrid_wins))
    res = stats.mannwhitneyu(gel_prod_cod, hybrid_prod_cod, alternative="two-sided")
    print("p-value for Gel = Hybrid: {}".format(res.pvalue))