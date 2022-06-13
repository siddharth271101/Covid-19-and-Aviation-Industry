from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, broadcast, count, first, to_date, lag, translate
from pyspark.sql.window import Window

def create_spark_session(app_name: str) -> SparkSession:
    """ Create a spark session.
    """
    ss = SparkSession.builder.master('local').appName(app_name).getOrCreate()
    return ss

def output_result(df: DataFrame, output_location: str, output_folder: str) -> None:
    """ Save the DataFrame <df> as a csv file in the location specified by
    <output_location>.
    """
    # condense all data points in one single file
    df.coalesce(1).write.parquet(path=output_location + output_folder,
                             mode='overwrite')

def enrich_flight_data(df_flights, df_airports):
    """Adds airport and flight type data to dataframe.

    Args:
        df_flights (Dataframe): Opensky dataframe
        df_airports (Dataframe): Airports information dataframe

    Returns:
        Dataframe: Flight dataset enhanced with airport location and flight type
    """

    # enrich data with airports info
    df_flights = df_flights.join(broadcast(df_airports.select(
        col('airport').alias('origin'),
        col('country_code').alias('origin_country_code'),
        col('type').alias('origin_airport_type')
    )), 'origin', how='left') \
        .join(broadcast(df_airports.select(
            col('airport').alias('destination'),
            col('country_code').alias('destination_country_code'),
            col('type').alias('destination_airport_type')
        )), 'destination', how='left')

    # classify the flight as domestic or international based on origin and destination countries
    # if either source or destination is None, we assume the flight is domestic
    flight_type_cond = when(col('origin_country_code').isNull() | col('destination_country_code').isNull(), 'domestic') \
        .otherwise(when(col('origin_country_code') == col('destination_country_code'), 'domestic')
                   .otherwise('international'))
    df_flights = df_flights.withColumn('type', flight_type_cond)

    return df_flights


def filter_airport_types(df_flights):
    """Remove with airports of type balloonport, seaplane_base, heliport, closed. 
    These types are filtered after joining with dataset because we don't want to 
    drop that have either origin or destination null.

    Args:
        df_flights (Dataframe): Flights dataframe

    Returns:
        Dataframe: Flight dataset not originating or terminating at balloonport, seaplane_base, heliport, closed airport types.
    """
    # we'll filter with origin or destination airports with type balloonport, seaplane_base, heliport, closed.
    # These airport types appear to be more of recreational type and should be irrelevant to our analysis.
    # Small, medium and large airports can also have recreational and charter, but we can't identify them.
    ap_types_allowed = ['large_airport', 'medium_airport', 'small_airport']
    df_flights = df_flights \
        .filter(col('origin_airport_type').isin(ap_types_allowed) | col('origin_airport_type').isNull()) \
        .filter(col('destination_airport_type').isin(ap_types_allowed) | col('destination_airport_type').isNull()) \
        .drop('origin_airport_type', 'destination_airport_type')

    return df_flights


def aggregate_flights(df_flights):
    """Aggregates flight data to traffic in and out of airports.

    Args:
        df_flights (Dataframe): Flights data

    Returns:
        Dataframe: Aggregated flight traffic for airports
    """

    # calculate traffic to and from each airport per day
    # for every flight that leaves an origin airport, we can count that as an outbound flight for the airport
    # similarly for destination airport, we count it as inbound
    df_outbound = df_flights \
        .filter(col('origin').isNotNull()) \
        .groupBy(col('date'), col('origin').alias('airport'), col('type')) \
        .agg(count('callsign').alias('outbound'))
    df_inbound = df_flights \
        .filter(col('destination').isNotNull()) \
        .groupBy(col('date'), col('destination').alias('airport'), col('type')) \
        .agg(count('callsign').alias('inbound'))

    # join the inbound and outbound traffic for all airports.
    # notice that since we've filtered origin and destination for nulls, we won't have full join where both are null.
    # pivot the flight type to convert it into columns.
    df_all_traffic = df_outbound.join(df_inbound, ['date', 'airport', 'type'], how='full') \
        .groupBy('date', 'airport') \
        .pivot('type') \
        .agg(first('inbound').alias('inbound'), first('outbound').alias('outbound')) \
        .na.fill(0) \
        .cache()

    return df_all_traffic


def flights_transform(df_flights, df_airports):
    """Enhances, filters and aggregates flight data to calculate airport traffic.

    Args:
        df_flights (Dataframe): Flights data
        df_airports (Dataframe): Airports data

    Returns:
        Dataframe: Airport traffic data
    """
    df_flights = enrich_flight_data(df_flights, df_airports)
    df_flights = filter_airport_types(df_flights)
    df_traffic = aggregate_flights(df_flights)
    return df_traffic


def airports_transform(df_airports, df_iban):
    """Merges airport data with country code data

    Args:
        df_airports (Dataframe): Airports data
        df_iban (Dataframe): Country code data

    Returns:
        Dataframe: Airports data with country information
    """
    return df_airports.join(broadcast(df_iban), 'country_code')

def clean_flights(df_os_raw):
    """Clean data

    Args:
        df_os_raw (Dataframe): Raw OpenSky dataset

    Returns:
        Dataframe: Flights data with relevant columns only
    """
    df = df_os_raw.select(
        col('callsign'), 
        col('origin'), 
        col('destination'), 
        to_date(col('day')).alias('date')
        # col('distance')
    )
    return df

def clean_airports(df_airports_raw):
    """Cleans airport dataset

    Args:
        df_airports_raw (Dataframe): Raw airports dataset

    Returns:
        Dataframe: cleaned airports dataset
    """
    df = df_airports_raw \
        .select(
            col('ident').alias('airport'), 
            col('type'), 
            col('name'), 
            col('iso_country').alias('country_code'), 
            col('iso_region').alias('region'), 
#             col('continent'), 
            col('coordinates')
        )
    return df

def clean_continents_data(df_continents):
    """Cleans country codes dataset

    Args:
        df_iban (Dataframe): Raw country code dataset

    Returns:
        Dataframe: dataset with columns renamed appropriately
    """

    df  = df_continents.select(
        col('name').alias('country_name'),
        col('alpha-2').alias('country_code'),
        col('region').alias('continent')
        )
    return df

def clean_states_data(df_states):
    """Cleans country codes dataset

    Args:
        df_iban (Dataframe): Raw country code dataset

    Returns:
        Dataframe: dataset with columns renamed appropriately
    """
    df = df_states.withColumn('code', translate('code', '.', '-'))
    return df

def clean_country_codes(df_iban):
    """Cleans country codes dataset

    Args:
        df_iban (Dataframe): Raw country code dataset

    Returns:
        Dataframe: dataset with columns renamed appropriately
    """
    df = df_iban \
        .select(
            col('Code').alias('country_code'),
            col('Name').alias('country')
        )
    return df

def clean_covid_data(df_covid_raw):
    """Cleans JHU COVID dataset

    Args:
        df_covid_raw (Dataframe): Raw JHU Enigma COVID dataset

    Returns:
        Dataframe: cleaned COVID dataset
    """
    df_covid = df_covid_raw.select(
        to_date(col('Date')).alias('date'),
        col('Country/Region').alias('country'),
        col('Province/State').alias('province_state'),
        col('Confirmed').cast('int').alias('confirmed'),
        col('Deaths').cast('int').alias('deaths'),
        col('Recovered').cast('int').alias('recovered')
    ).na.fill(0)

    window = Window.partitionBy("country").orderBy("date")

    df_covid = df_covid.withColumn("confirmed", col("confirmed") - lag(col("confirmed"), 1, 0).over(window))
    df_covid = df_covid.withColumn("deaths", col("deaths") - lag(col("deaths"), 1, 0).over(window))
    df_covid = df_covid.withColumn("recovered", col("recovered") - lag(col("recovered"), 1, 0).over(window))
    return df_covid


if __name__ == '__main__':
    scSpark = create_spark_session("covid19_airtraffic_data")
    scSpark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    os_path = "s3://{your-bucket-name}/raw/opensky/*.csv"
    airports_path = "s3://{your-bucket-name}/airports.csv"
    covid_path = "s3://-/raw/covid.csv"
    countries_path = "s3://{your-bucket-name}/raw/countries.json"
    continents_path = "s3://{your-bucket-name}/raw/continents.csv"
    ind_states_path = "s3://{your-bucket-name}/raw/india-states.csv"

    df_os_raw = scSpark.read.csv(os_path, header='true', sep=',', inferSchema=False)
    df_airports_raw = scSpark.read.csv(airports_path, header='true', sep=',', inferSchema=False)
    df_covid_raw = scSpark.read.csv(covid_path, header='true', sep=',', inferSchema=False)
    df_countries_raw = scSpark.read.json(countries_path)
    df_continents_raw = scSpark.read.csv(continents_path, header='true', sep=',', inferSchema=False)
    df_ind_states_raw = scSpark.read.csv(ind_states_path, header='true', sep=',', inferSchema=False)

    df_flights = clean_flights(df_os_raw)
    df_airports = clean_airports(df_airports_raw)
    df_covid = clean_covid_data(df_covid_raw)
    df_countries = clean_country_codes(df_countries_raw)
    df_continents = clean_continents_data(df_continents_raw)
    df_ind_states = clean_states_data(df_ind_states_raw)

    df_airports = airports_transform(df_airports, df_countries)
    df_all_traffic = flights_transform(df_flights, df_airports)

    OUTPUT_LOCATION = 's3://{your-bucket-name}/output/'

    # save the above results as parquet files in the bucket
    output_result(df_airports, OUTPUT_LOCATION, 'airports_dim')
    output_result(df_all_traffic, OUTPUT_LOCATION, 'all_traffic_fact')
    output_result(df_covid, OUTPUT_LOCATION, 'covid_fact')
    output_result(df_countries, OUTPUT_LOCATION, 'countries_dim')
    output_result(df_continents, OUTPUT_LOCATION, 'continents_dim')
    output_result(df_countries, OUTPUT_LOCATION, 'ind_states_dim')