from datetime import datetime, timezone
import file_handler
import sql_handler
import api_client

name = "inbar_shalitin"


def initial_orbital_table(cities_data):
    for city in cities_data:
        data = []
        for i in range(len(cities_data[city])):
            timestamp = cities_data[city][i]["risetime"]
            timestamp_to_utc = datetime.fromtimestamp(timestamp, timezone.utc)
            row = (city, timestamp_to_utc, cities_data[city][i]["duration"])
            data.append(row)

        sql_handler.insert_to_table("orbital_data_" + name + "(city,timestamp,duration_seconds)", "(%s,%s,%s)", data)


def initial_city_stats_table(cities_data):
    data = []
    for city in cities_data:
        row = (city, 0)
        data.append(row)

    sql_handler.reset_city_stats_table(data)


def create_and_run_stored_procedure():
    sp_name = sql_handler.create_count_avg_sp()
    if sp_name is not None:
        return sql_handler.run_stored_procedure(sp_name)


def combine_and_store_data():
    if sql_handler.combine_data():
        columns = sql_handler.get_col_names("union_table")
        data = sql_handler.get_data_from_table("union_table")
        file_handler.store_results_to_csv_file(columns, data, "combined_data")
        sql_handler.delete_table("union_table")


sql_handler = sql_handler.SQLHandler(name)
api_client = api_client.APIClient()
cities_data = api_client.parse_json()
sql_handler.connect_db()
sql_handler.create_orbital_table()
initial_orbital_table(cities_data)
initial_city_stats_table(cities_data)
create_and_run_stored_procedure()
combine_and_store_data()
