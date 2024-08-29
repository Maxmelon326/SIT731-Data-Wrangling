# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: title,-all
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# Apply SQL with Pandas
# * Name:LI WAN
# * Student Number:223718804
# * E-mail:s223718804@deakin.edu.au
# * Course:SIT731
#
# ---

# %% ### Introduction [markdown]
# The report delves into the analysis of the "nycflights13" dataset, which contains comprehensive information about 336,776 flights departing in the year 2013 from three major New York airports: EWR, JFK, and LGA. These flights were destined for locations within the United States, Puerto Rico, and the American Virgin Islands. The primary objective of this study is to showcase various techniques for performing data queries in Python. Specifically, we will explore how to achieve identical query results using SQLite with pandas and pure pandas methods. Moreover, we will conduct a comparative analysis of the execution times of these different approaches. Through these tasks, we aim to demonstrate the versatility and efficiency of Python for data analysis and querying in the context of real-world flight data.

# %%
import pandas as pd
import sqlite3
import timeit

# Create and connect SQLite
conn = sqlite3.connect('my_database.db')

# 4 CSV files: 'airlines.csv', 'weather.csv', 'airports.csv', 'planes.csv'
csv_files = ['airlines.csv', 'weather.csv', 'airports.csv', 'planes.csv', 'flights.csv']

# Import CSV files to入SQLite database
for file in csv_files:
    if file == 'flights.csv':
        #Read file from local disk  'flights.csv' tranfer to Pandas DataFrame
        local_path = r'E:\2-学习\1-Deakin\23-T3\SIT731\作业\task6\flights.csv'
        df = pd.read_csv(local_path, comment='#')
    else:
        #read from CSV files to Pandas DataFrame
        df = pd.read_csv(f"https://github.com/Maxmelon326/SIT731/raw/main/{file}", comment='#')
    
    # Read data to SQLite tables
    df.to_sql(file.replace('.csv', ''), conn, index=False, if_exists='replace')

# %%
airlines = pd.read_csv("https://github.com/Maxmelon326/SIT731/raw/main/airlines.csv", comment='#')
airlines.head(3)
# %%
weather = pd.read_csv("https://github.com/Maxmelon326/SIT731/raw/main/weather.csv", comment='#')
weather.head(3)
# %%
airports= pd.read_csv("https://github.com/Maxmelon326/SIT731/raw/main/airports.csv",comment="#")
airports.head(3)
# %%
planes= pd.read_csv("https://github.com/Maxmelon326/SIT731/raw/main/planes.csv",comment="#")
planes.head(3)
# %%
local_path = r'E:\2-学习\1-Deakin\23-T3\SIT731\作业\task6\flights.csv'
flights= pd.read_csv(local_path, comment='#')
flights.head(3)


# %% [markdown]
# The following 17 tasks will primarily explore how to perform data queries in Python. The goal is to demonstrate how to achieve the same query results using SQLite with pandas and pure pandas methods. Additionally, we will compare the execution times of different approaches.  
# Task 1 is to retrieve distinct engine values from the "planes" table. 

# %%
#Task1
# SQLite Solution
def sqlite_solution():
    task1_sql = pd.read_sql_query("SELECT DISTINCT engine FROM planes", conn)
    return task1_sql.sort_values(by='engine').reset_index(drop=True)

# Pure Pandas Solution
def pure_pandas_solution():
    task1_my = pd.DataFrame({'engine': planes['engine'].drop_duplicates()}).sort_values(by='engine').reset_index(drop=True)
    return task1_my

# Execute the solutions
task1_sql = sqlite_solution()
task1_my = pure_pandas_solution()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task1_sql, task1_my)

# Display the first few rows of task1_my
task1_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")


# %% [markdown]
# Task 2 is to retrieve distinct combinations of 'type' and 'engine' from the 'planes' table, and the results are sorted by 'type' and 'engine'.

# %%
# Task2
def sqlite_solution2():
    task2_sql = pd.read_sql_query("SELECT DISTINCT type, engine FROM planes", conn)
    return task2_sql.sort_values(by=['type', 'engine']).reset_index(drop=True)

def pure_pandas_solution2():
    task2_my = planes[['type', 'engine']].drop_duplicates().sort_values(by=['type', 'engine']).reset_index(drop=True)
    return task2_my

# Execute the solutions
task2_sql = sqlite_solution2()
task2_my = pure_pandas_solution2()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task2_sql, task2_my)

task2_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution2, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution2, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")


# %% [markdown]
# Task 3 involves grouping the records in the planes table by the 'engine' column, counting the number of planes for each engine type, and returning a result set containing the engine types and their respective counts. This is achieved by using the GROUP BY clause to group by the 'engine' field and using the COUNT(*) function to calculate the number of rows in each group. The result of the query consists of two columns: 'engine,' representing the engine type, and 'count,' representing the count of planes for each engine type.

# %%
# Task3
def sqlite_solution3():
    task3_sql = pd.read_sql_query("SELECT COUNT(*) AS count, engine FROM planes GROUP BY engine", conn)
    return task3_sql[['engine', 'count']].sort_values(by=['count', 'engine']).reset_index(drop=True)

def pure_pandas_solution3():
    task3_my = planes.groupby('engine').size().reset_index(name='count').sort_values(by=['count', 'engine']).reset_index(drop=True)
    return task3_my[['engine', 'count']]

# Execute the solutions
task3_sql = sqlite_solution3()
task3_my = pure_pandas_solution3()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task3_sql, task3_my)
# Display the first few rows of task3_my
task3_my.head()


# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution3, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution3, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")


# %% [markdown]
# Task 4 is designed to count the number of planes in a database, grouped by their engine type and model type. It does this by selecting all entries from the 'planes' table, grouping them first by the 'engine' column and then by the 'type' column. For each group, it counts the total number of entries. Finally, the results are sorted by the 'engine' and 'type' columns.

# %%
# Task4
def sqlite_solution4():
    task4_sql = pd.read_sql_query("SELECT COUNT(*), engine, type FROM planes GROUP BY engine, type", conn)
    return task4_sql[['engine', 'type', 'COUNT(*)']].sort_values(by=['engine', 'type']).reset_index(drop=True)

def pure_pandas_solution4():
    task4_my = planes.groupby(['engine', 'type']).size().reset_index(name='COUNT(*)')
    # Reorder columns to match task4_sql
    task4_my = task4_my[['engine', 'type', 'COUNT(*)']].sort_values(by=['engine', 'type']).reset_index(drop=True)
    return task4_my

# Execute the solutions
task4_sql = sqlite_solution4()
task4_my = pure_pandas_solution4()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task4_sql, task4_my)
# Display the first few rows of task4_my
task4_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time4 = timeit.timeit(sqlite_solution4, number=100)

# Measure time for pure pandas solution
pandas_time4 = timeit.timeit(pure_pandas_solution4, number=100)


# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")


# %% [markdown]
# Task 5 solutions summarize the year column of a dataset containing aircraft information, grouped by the engine and manufacturer categories. They calculate the minimum, average, and maximum values of the year column within each group, and the results are sorted by engine and manufacturer.

# %%
#Task 5
def sqlite_solution5():
    task5_sql = pd.read_sql_query(
        "SELECT MIN(year), AVG(year), MAX(year), engine, manufacturer FROM planes GROUP BY engine, manufacturer", conn)
    return task5_sql.round(2).sort_values(by=['engine', 'manufacturer']).reset_index(drop=True)

def pure_pandas_solution5():
    grouped = planes.groupby(['engine', 'manufacturer'])

    task5_my = grouped['year'].agg(
        ['min', 'mean', 'max']
    ).rename(columns={'min': 'MIN(year)', 'mean': 'AVG(year)', 'max': 'MAX(year)'})


    # Reorder to match SQL
    task5_my = task5_my.reset_index()
    task5_my = task5_my[['MIN(year)', 'AVG(year)', 'MAX(year)', 'engine', 'manufacturer']]
    task5_my = task5_my.round(2).sort_values(by=['engine', 'manufacturer']).reset_index(drop=True)

    return task5_my
# Execute the solutions
task5_sql = sqlite_solution5()
task5_my = pure_pandas_solution5()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task5_sql, task5_my)

# Display the first few rows of task5_my
task5_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution5, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution5, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")


# %% [markdown]
# Task 6 is to retrieve records from table "planes". It specifically selects all columns for rows where the "speed" column is not null. The result includes all columns and rows that meet this condition.

# %% #Task6
def sqlite_solution6():
    task6_sql = pd.read_sql_query("SELECT * FROM planes WHERE speed IS NOT NULL", conn)
    return task6_sql

def pure_pandas_solution6():
    task6_my = planes[planes['speed'].notnull()]
    return task6_my.sort_values(by=['tailnum']).reset_index(drop=True)


# Execute the solutions
task6_sql = sqlite_solution6()
task6_my = pure_pandas_solution6()

# Check if the result DataFrames are equal to the SQL result
pd.testing.assert_frame_equal(task6_sql, task6_my)

# Display the first few rows of task6_my
task6_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution6, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution6, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 7 is to retrieve aircraft tail numbers from table "planes". It selects tail numbers for planes with seat capacities between 150 and 210 seats and manufacturing years equal to or greater than 2011

# %% #Task7
def sqlite_solution7():
    task7_sql = pd.read_sql_query("SELECT tailnum FROM planes WHERE seats BETWEEN 150 AND 210 AND year >= 2011", conn)
    return task7_sql

def pure_pandas_solution7():
    task7_my = planes[(planes['seats'].between(150, 210)) & (planes['year'] >= 2011)][['tailnum']]
    return task7_my.sort_values('tailnum').reset_index(drop=True)

# Execute the solutions
task7_sql = sqlite_solution7()
task7_my = pure_pandas_solution7()

# Check if the result DataFrames are equal
pd.testing.assert_frame_equal(task7_sql, task7_my)

task7_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")


# %% [markdown]
# Task8 is to retrieve aircraft information from table "planes". It selects aircraft with manufacturers either 'BOEING,' 'AIRBUS,' or 'EMBRAER' and seat capacities greater than 390. The result includes columns for aircraft tail numbers ('tailnum'), manufacturer names ('manufacturer'), and seat capacities ('seats'). 

# %%
# Task8 
def sqlite_solution8():
    task8_sql = pd.read_sql_query("SELECT tailnum, manufacturer, seats FROM planes WHERE manufacturer \
    IN ('BOEING', 'AIRBUS', 'EMBRAER') AND seats > 390", conn)
    return task8_sql

def pure_pandas_solution8():
    manufacturers = ['BOEING', 'AIRBUS', 'EMBRAER']
    task8_my = planes[(planes['manufacturer'].isin(manufacturers)) & (planes['seats'] > 390)]\
    [['tailnum', 'manufacturer', 'seats']]
    return task8_my.reset_index(drop=True)  # Reset the index

# Execute the solutions
task8_sql = sqlite_solution8()
task8_my = pure_pandas_solution8()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task8_sql, task8_my)

task8_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution8, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution8, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 9 is to retrieve unique combinations of aircraft manufacturing years ('year') and seat capacities ('seats') from table "planes". It selects only those rows where the manufacturing year is greater than or equal to 2012. The results are sorted in ascending order by year and in descending order by seat capacity. 

# %%
#Task9
def sqlite_solution9():
    task9_sql = pd.read_sql_query("SELECT DISTINCT year, seats FROM planes WHERE year >= 2012 ORDER BY year ASC, seats DESC", conn)
    return task9_sql  # Return the DataFrame

def pure_pandas_solution9():
    task9_my = planes[planes['year'] >= 2012][['year', 'seats']].drop_duplicates().\
    sort_values(by=['year', 'seats'], ascending=[True, False])
    return task9_my.reset_index(drop=True)  # Return the DataFrame

# Execute the solutions
task9_sql = sqlite_solution9()
task9_my = pure_pandas_solution9()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task9_sql, task9_my)

task9_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution9, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution9, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 10 is to retrieve unique combinations of aircraft manufacturing years ('year') and seat capacities ('seats') from table "planes". It selects only those rows where the manufacturing year is greater than or equal to 2012. The results are sorted in descending order by seat capacity and in ascending order by year.

# %%
#Task 10
def sqlite_solution10():
    task10_sql = pd.read_sql_query("SELECT DISTINCT year, seats FROM planes WHERE year >= 2012 \
    ORDER BY seats DESC, year ASC", conn)
    return task10_sql

def pure_pandas_solution10():
    task10_my = planes[planes['year'] >= 2012]\
    [['year', 'seats']].drop_duplicates().sort_values(by=['seats', 'year'], ascending=[False, True])
    return task10_my.reset_index(drop=True)  # Reset the index

# Execute the solutions
task10_sql = sqlite_solution10()
task10_my = pure_pandas_solution10()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task10_sql, task10_my)

task10_my.head()


# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution10, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution10, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 11 is to retrieve data from table "planes". It counts the number of rows for each unique manufacturer where the seat capacity of the aircraft is greater than 200. The results are grouped by manufacturer, and the count of aircraft meeting the specified condition is calculated. The goal is to provide a count of such aircraft for each manufacturer.

# %%
#Task 11
def sqlite_solution11():
    task11_sql = pd.read_sql_query("SELECT manufacturer, COUNT(*) FROM planes WHERE seats > 200 GROUP BY manufacturer", conn)
    return task11_sql  # Add this line to return the DataFrame

def pure_pandas_solution11():
    task11_my = planes[planes['seats'] > 200].groupby('manufacturer').size().reset_index(name='COUNT(*)')
    return task11_my

# Execute the solutions
task11_sql = sqlite_solution11()
task11_my = pure_pandas_solution11()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task11_sql, task11_my)

task11_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution11, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution11, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 12 is to retrieve data from table "planes". It counts the number of aircraft for each unique manufacturer and groups the results by manufacturer. Then, it applies a filter condition using the HAVING clause to select only those manufacturers with a count greater than 10 aircraft.

# %%
#Task 12
def sqlite_solution12():
    task12_sql = pd.read_sql_query("SELECT manufacturer, COUNT(*) FROM planes GROUP BY manufacturer HAVING COUNT(*) > 10", conn)
    return task12_sql

def pure_pandas_solution12():
    task12_my = planes.groupby('manufacturer').size().reset_index(name='COUNT(*)')
    task12_my = task12_my[task12_my['COUNT(*)'] > 10]
    return task12_my.reset_index(drop=True)

# Execute the solutions
task12_sql = sqlite_solution12()
task12_my = pure_pandas_solution12()

# Check if the result DataFrame is equal to the SQL result
pd.testing.assert_frame_equal(task12_sql, task12_my)

task12_my.head()

# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution12, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution12, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 13 is to retrieve data from table "planes". It counts the number of aircraft for each unique manufacturer, but it only includes aircraft with seat capacities greater than 200. The results are grouped by manufacturer, and a filter condition using the HAVING clause is applied to select manufacturers with a count of aircraft greater than 10. 

# %%
#Task 13
def sqlite_solution13():
    task13_sql = pd.read_sql_query("SELECT manufacturer, COUNT(*) FROM planes WHERE seats > 200 \
    GROUP BY manufacturer HAVING COUNT(*) > 10", conn)
    return task13_sql

def pure_pandas_solution13():
    task13_my = planes[planes['seats'] > 200].groupby('manufacturer').size().reset_index(name='COUNT(*)')
    task13_my = task13_my[task13_my['COUNT(*)'] > 10]
    return task13_my.reset_index(drop=True)

# Execute the solutions
task13_sql = sqlite_solution13()
task13_my = pure_pandas_solution13()


task13_my.head()

# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 14 is to retrieve data from table "planes". It counts the number of aircraft for each unique manufacturer and assigns the alias "howmany" to this count. The results are then grouped by manufacturer, and the groups are sorted in descending order based on the "howmany" count. Finally, a limit of 10 is applied to the result set, ensuring that only the top 10 manufacturers with the highest number of aircraft are included. 

# %%
#Task 14
def sqlite_solution14():
    task14_sql = pd.read_sql_query("SELECT manufacturer, COUNT(*) AS howmany FROM planes \
    GROUP BY manufacturer ORDER BY howmany DESC LIMIT 10", conn)
    return task14_sql

def pure_pandas_solution14():
    task14_my = planes.groupby('manufacturer').size().reset_index(name='howmany')
    task14_my = task14_my.sort_values(by='howmany', ascending=False).head(10)
    return task14_my.reset_index(drop=True)

# Execute the solutions
task14_sql = sqlite_solution14()
task14_my = pure_pandas_solution14()

task14_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution14, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution14, number=100)


# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 15 performs a left join between two database tables, "flights" and "planes," based on the "tailnum" column. It retrieves flight data from the "flights" table and aircraft information (year, speed, and seats) from the "planes" table, joining them on the common "tailnum" key. This operation creates a merged dataset that includes flight details alongside corresponding aircraft details.

# %%
#Task 15
def sqlite_solution15():
    task15_sql = pd.read_sql_query("""SELECT flights.*, planes.year AS plane_year, planes.speed \
    AS plane_speed, planes.seats AS plane_seats FROM flights LEFT JOIN planes ON flights.tailnum=planes.tailnum""", conn)
    return task15_sql

def pure_pandas_solution15():
    #select columns before merge
    planes_selected = planes[['tailnum', 'year', 'speed', 'seats']].rename(
        columns={'year': 'plane_year', 'speed': 'plane_speed', 'seats': 'plane_seats'}
    )

    # left join
    task15_my = pd.merge(flights, planes_selected, on='tailnum', how='left')

    return task15_my

# Execute the solutions
task15_sql = sqlite_solution15()
task15_my = pure_pandas_solution15()

task15_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution15, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution15, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 16 is to retrieve data from multiple tables by first selecting distinct combinations of "carrier" and "tailnum" from the "flights" table and then performing inner joins with the "planes" and "airlines" tables based on these combinations. It combines columns from all three tables and returns the result.


# %%
#Task 16
def sqlite_solution16():
    task16_sql = pd.read_sql_query("""SELECT planes.*, airlines.* FROM (SELECT DISTINCT carrier, tailnum FROM flights) 
    AS cartail INNER JOIN planes ON cartail.tailnum=planes.tailnum INNER JOIN airlines ON cartail.carrier=airlines.carrier""", conn)
    return task16_sql.reset_index(drop=True)

def pure_pandas_solution16():
    # Select distinct 'carrier' and 'tailnum' and drop duplicates
    distinct_cartail = flights[['carrier', 'tailnum']].drop_duplicates()

    # Select only necessary columns from planes and airlines DataFrames
    planes_selected = planes[['tailnum', 'year', 'type', 'manufacturer', 'model', 'engines', 'seats', 'speed', 'engine']]
    airlines_selected = airlines[['carrier', 'name']]

    # Merge distinct_cartail with selected columns from planes on 'tailnum'
    merged_with_planes = pd.merge(distinct_cartail, planes_selected, on='tailnum', how='inner')

    # Merge the result with selected columns from airlines on 'carrier'
    merged_with_airlines = pd.merge(merged_with_planes, airlines_selected, on='carrier', how='inner')

    # Reorder columns to match the SQL output
    reordered_columns = ['tailnum', 'year', 'type', 'manufacturer', 'model', 'engines', 'seats',
                         'speed', 'engine', 'carrier', 'name']
    task16_my = merged_with_airlines[reordered_columns]

    # Sort the resulting DataFrame
    task16_my = task16_my.sort_values(by=['tailnum', 'carrier'])

    return task16_my.reset_index(drop=True)

# Execute the solutions
task16_sql = sqlite_solution16()
task16_my = pure_pandas_solution16()

# Asserting the equality of the DataFrames
pd.testing.assert_frame_equal(task16_sql, task16_my)

# Display the first few rows of task16_my
task16_my.head()

# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution16, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution16, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# Task 17 combines data from two tables, "flights" and "weather," focusing on flights originating from Newark Airport (EWR). It selects flight information from the "flights" table for flights departing from EWR and joins it with weather data from the "weather" table, calculating the average temperature (atemp) and humidity (ahumid) for each year, month, and day. 


# %%
#Task 17
def sqlite_solution17():
    task17_sql = pd.read_sql_query(
    """SELECT
    flights2.*,
    atemp,
    ahumid
    FROM (
    SELECT * FROM flights WHERE origin='EWR'
    ) AS flights2
    LEFT JOIN (
    SELECT
    year, month, day,
    AVG(temp) AS atemp,
    AVG(humid) AS ahumid
    FROM weather
    WHERE origin='EWR'
    GROUP BY year, month, day
    ) AS weather2
    ON flights2.year=weather2.year
    AND flights2.month=weather2.month
    AND flights2.day=weather2.day""", conn)

    return task17_sql

 
def pure_pandas_solution17():
    # Select flights originating from 'EWR'
    flights2 = flights[flights['origin'] == 'EWR'][[
        'year', 'month', 'day', 'dep_time', 'sched_dep_time', 'dep_delay', 'arr_time',
        'sched_arr_time', 'arr_delay', 'carrier', 'flight', 'tailnum', 'origin',
        'dest', 'air_time', 'distance', 'hour', 'minute', 'time_hour'
    ]]

    # Group weather data by year, month, and day and calculate average temperature and humidity
    weather2 = weather[weather['origin'] == 'EWR'].groupby(['year', 'month', 'day']).agg({
        'temp': 'mean',
        'humid': 'mean'
    }).reset_index()

    # Merge flights and weather on year, month, and day
    task17_my = pd.merge(flights2, weather2, on=['year', 'month', 'day'], how='left')

    # Rename columns to match the expected result
    task17_my.rename(columns={'temp': 'atemp', 'humid': 'ahumid'}, inplace=True)

    return task17_my.reset_index(drop=True)

# Execute the solutions
task17_sql = sqlite_solution17()
task17_my = pure_pandas_solution17()

pd.testing.assert_frame_equal(task17_sql, task17_my)

task17_my.head()
# %%
# Measure time for SQLite3 (through pandas) solution
sqlite_time = timeit.timeit(sqlite_solution17, number=100)

# Measure time for pure pandas solution
pandas_time = timeit.timeit(pure_pandas_solution17, number=100)

# Display results
print(f"SQLite3 time: {sqlite_time} seconds")
print(f"Pandas  time: {pandas_time} seconds")

# %% [markdown]
# ### Conclusion
# From the above examples,SQLite3 and pandas can both realize data query function. Generally, SQLite3 consumes more computation time, especially when it comes to merging multiple tables, which seems to be more efficient in pyhon when it comes to checking the pandas method for databases.

# %% [markdown]
# Reference
# https://en.wikipedia.org/wiki/Body_mass_index
