import itertools
import numpy as np
import regex as re
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

#!pip install pydap
from pydap.client import open_url
from pydap.cas.urs import setup_session

#!pip install geopy
from geopy.geocoders import Nominatim
locator = Nominatim(user_agent="geoapiExercises")

import warnings
warnings.simplefilter("ignore")


#-------------------------------------#

# Class: Scrapper for NASA Earth Data
class nasa_scrapper():
        
    def __init__(self, username=None, 
                 password=None):
        
        if username is None or password is None:
            self.un, self.pwd = open("Requirements.txt","r").read().split(",")
        else:
            self.un, self.pwd = username, password
    
  
  
    def __get_location(self, lat, lon):
        return locator.reverse(f"{lat},{lon}")
    
  
  
    def get_data(self, url, variable):
        """
        Arguments:
            url: (str) a link to the desired dataset at GES DISC data server
            variable: (str) name of the variable of interest in the dataset
        
        """
        
        # establishes a secure connection with the database server
        try:
            session = setup_session(self.un, 
                                    self.pwd, 
                                    check_url=url)
            dataset = open_url(url, 
                               session=session)
            
        except AttributeError as e:
            print(f"{e}: Kindly check the url.")
            
        print("Status: Connection is successfully extablished.")
            
        # gets the child objects of dataset 
        children = [i.name for i in dataset.children()]
        data = dataset.get(variable)
        
        # gets the time, latitude and longitude informations
        time = data["time"][:].data
        lat = data["lat"][:].data
        lon = data["lon"][:].data
        var = data[variable][:].data
        
        print("Status: Raw data retrieved successfully.")
        
        # time of start of observation
        event_start_time = datetime(year=2010, month=1, day=1, hour=0, minute=0)
        time_column = [event_start_time + timedelta(days=n*(7*365/12)) for n in range(len(time))]
         
        map_data = [list(time_column),list(lat),list(lon)]
        map_tuple_list = list(itertools.product(*map_data))
        
        
        d = pd.DataFrame(map_tuple_list, 
                         columns = ["timestamp",
                                    "latitude",
                                    "longitude"])
        
        d[variable] = var.flatten()
        
        d["location"] = d.apply(lambda row: self.__get_location(row["latitude"], 
                                                                row["longitude"]), 
                                axis=1)
        
        print("Status: Fetching geolocation...")
        
        country, county, state, district, pincode, region_code, country_code = [],[],[],[],[],[],[]
        
        for i in range(d.shape[0]):
            if d.location[i] != None:
                loc = self.__get_location(d.latitude[i], d.longitude[i])
                
                country.append(loc["country"])
                county.append(loc["county"])
                state.append(loc["state"])
                district.append(loc["state_district"])
                pincode.append(loc["postcode"])
                region_code.append(loc["ISO3166-2-lvl4"])
                country_code.append(loc["country_code"])
            else:
                country.append(None)
                county.append(None)
                state.append(None)
                district.append(None)
                pincode.append(None)
                region_code.append(None)
                country_code.append(None)
                
        d["country"] = country
        d["county"] = county
        d["state"] = state
        d["district"] = district
        d["pincode"] = pincode
        d["region_code"] = region_code
        d["country_code"] = country_code
        
        print("Status: Information retreival completed.")
       
        
        return d
        

url = "https://acdisc.gsfc.nasa.gov/opendap/CMS/CMSFluxFire.2/CMS_Flux_Fire_2010_v2.nc"
scrapper = nasa_scrapper()
d = scrapper.get_data(url, "flux")


print(d)