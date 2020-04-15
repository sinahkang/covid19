### Dependencies 
import pandas as pd 
import requests 
from sqlalchemy import create_engine
import datetime

### Global Covid19 Data
# Covid-19 Confirmed Cases
confirmed_url = "https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
confirmed_html = requests.get(confirmed_url).text 
confirmed_df = pd.read_html(confirmed_html)[0]
confirmed_df = confirmed_df.iloc[:, 1:]
confirmed_df = confirmed_df.melt(id_vars=['Country/Region', 'Province/State', 'Lat', 'Long'])
confirmed_df = confirmed_df.rename(columns={"Country/Region": "country_region", "Province/State": "province_state",  "Lat": "lat", "Long": "long", "variable":"date", "value": "confirmed"})

# Covid-19 Deaths
deaths_url = "https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
deaths_html = requests.get(deaths_url).text 
deaths_df = pd.read_html(deaths_html)[0]
deaths_df = deaths_df.iloc[:, 1:]
deaths_df = deaths_df.melt(id_vars=['Country/Region', 'Province/State', 'Lat', 'Long'])
deaths_df = deaths_df.rename(columns={"Country/Region": "country_region", "Province/State": "province_state", "Lat": "lat", "Long": "long", "variable":"date", "value": "deaths"})

# Covid-19 Recovered
recovered_url = "https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"
recovered_html = requests.get(recovered_url).text 
recovered_df = pd.read_html(recovered_html)[0]
recovered_df = recovered_df.iloc[:, 1:]
recovered_df = recovered_df.melt(id_vars=['Country/Region', 'Province/State', 'Lat', 'Long'])
recovered_df = recovered_df.rename(columns={"Country/Region": "country_region", "Province/State": "province_state", "Lat": "lat", "Long": "long", "variable":"date", "value": "recovered"})

# Merge three dataframes
global_df1 = pd.merge(confirmed_df, deaths_df, how="outer")
global_df2 = pd.merge(global_df1, recovered_df, how="outer")

# Transform "Date" column into datatime format
global_df2["date"] = pd.to_datetime(global_df2["date"]).dt.date

# Add column ISO3 
iso_url = "https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv"
iso_html = requests.get(iso_url).text
iso_df = pd.read_html(iso_html)[0]
iso3_df = iso_df[["iso3", "Country_Region", "Province_State", "Lat", "Long_"]].rename(columns={"Country_Region": "country_region", "Province_State": "province_state", "Lat":"lat", "Long_":"long"})
iso3_df = iso3_df.loc[iso3_df["province_state"].isnull()].drop("province_state", 1)

global_df2.drop(["lat", "long"], axis=1, inplace=True)
global_df2 = global_df2.groupby(["country_region", "date"]).sum().reset_index()
global_df2["case_fatality"] = round(global_df2["deaths"] / global_df2["confirmed"] * 100, 2)
global_df = pd.merge(global_df2, iso3_df, how="left")

### USA Covid19 Data
# USA confirmed cases
usa_confirmed_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
usa_confirmed_df = pd.read_csv(usa_confirmed_url)
usa_confirmed_df.drop(["UID", "iso2", "iso3", "code3", "FIPS", "Combined_Key"], axis=1, inplace=True)
usa_confirmed_df = usa_confirmed_df.melt(id_vars=['Country_Region', 'Province_State', "Admin2", 'Lat', 'Long_'])
usa_confirmed_df = usa_confirmed_df.rename(columns={"Country_Region": "country_region", "Province_State": "province_state", "Admin2":"county_city", "Lat": "lat", "Long_": "long", "variable":"date", "value": "confirmed"})

# USA deaths
usa_deaths_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
usa_deaths_df = pd.read_csv(usa_deaths_url)
usa_deaths_df.drop(["UID", "iso2", "iso3", "code3", "FIPS", "Combined_Key", "Population"], axis=1, inplace=True)
usa_deaths_df = usa_deaths_df.melt(id_vars=['Country_Region', 'Province_State', 'Admin2', 'Lat', 'Long_'])
usa_deaths_df = usa_deaths_df.rename(columns={"Country_Region": "country_region", "Province_State": "province_state", "Admin2":"county_city", "Lat": "lat", "Long_": "long", "variable":"date", "value": "deaths"})

# Merge USA dataframes
usa_df = pd.merge(usa_confirmed_df, usa_deaths_df, how="outer")
usa_df = usa_df.drop_duplicates()
usa_df["date"] = pd.to_datetime(usa_df["date"]).dt.date
usa_df["confirmed"] = usa_df["confirmed"].fillna(0)
usa_df["deaths"] = usa_df["deaths"].fillna(0)

# Connect to the "covid19" database in MySQL
HOSTNAME = "127.0.0.1"
PORT = 3306
USERNAME = "root"
PASSWORD = "kangsong87"
DIALECT = "mysql"
DRIVER = "pymysql"
DATABASE = "covid19"

connection_string = ( f"{DIALECT}+{DRIVER}://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}" )
engine = create_engine(connection_string)

# Create "global_covid19" and "usa_covid19" tables in the "covid19" database
global_df.to_sql(con=engine, name="global_covid19", if_exists="replace")
usa_df.to_sql(con=engine, name="usa_covid19", if_exists="replace")