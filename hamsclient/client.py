import pandas as pd
import geopy
import geopy.distance
from bs4 import BeautifulSoup
import json
import requests
import logging
import re


_LOGGER = logging.getLogger(__name__)

MS_BASE_URL = 'https://www.meteosuisse.admin.ch'
JSON_FORECAST_URL = 'https://app-prod-ws.meteoswiss-app.ch/v1/forecast?plz={}00&graph=false&warning=true'
MS_SEARCH_URL = 'https://www.meteosuisse.admin.ch/home/actualite/infos.html?ort={}&pageIndex=0&tab=search_tab'
CURRENT_CONDITION_URL= 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv'
STATION_URL = "https://data.geo.admin.ch/ch.meteoschweiz.messnetz-automatisch/ch.meteoschweiz.messnetz-automatisch_fr.csv"
MS_24FORECAST_URL = "https://www.meteosuisse.admin.ch/product/output/forecast-chart/{}/fr/{}00.json"
MS_24FORECAST_REF = "https://www.meteosuisse.admin.ch//content/meteoswiss/fr/home.mobile.meteo-products--overview.html"

class meteoSwissClient():
    def __init__(self,displayName=None,postcode=None,station=None):
        _LOGGER.debug("MS Client INIT")
        self._postCode = postcode
        self._station = station
        self._name = displayName
        self._allStations = None
        self._condition = None
        self._forecast = None
        _LOGGER.debug("INIT meteoswiss client : name = %s station = %s postcode = %s"%(self._name,self._station,self._postCode))


    def get_data(self):
        self.get_forecast()
        self.get_current_condition()
        return  {"name": self._name,"forecast": self._forecast, "condition":self._condition}

    def get_24hforecast(self):
        _LOGGER.debug("Start update 24h forecast data")
        s = requests.Session()
        #Forcing headers to avoid 500 error when downloading file
        s.headers.update({"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Encoding":"gzip, deflate, sdch",'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/1337 Safari/537.36'})
        searchUrl = MS_SEARCH_URL.format(self._postCode)
        _LOGGER.debug("Main URL : %s"%searchUrl)
        tmpSearch = s.get(searchUrl,timeout=10)

        soup = BeautifulSoup(tmpSearch.text,features="html.parser")
        widgetHtml = soup.find_all("section",{"id": "weather-widget"})
        jsonUrl = widgetHtml[0].get("data-json-url")
        jsonUrl = str(jsonUrl)
        version = jsonUrl.split('/')[5]
        forecastUrl = MS_24FORECAST_URL.format(version,self._postCode)
        _LOGGER.debug("Data URL : %s"%forecastUrl)
        s.headers.update({'referer': MS_24FORECAST_REF,"x-requested-with": "XMLHttpRequest","Accept": "application/json, text/javascript, */*; q=0.01","dnt": "1"})
        jsonData = s.get(forecastUrl,timeout=10)
        jsonData.encoding = "utf8"
        jsonDataTxt = jsonData.text

        jsonObj = json.loads(jsonDataTxt)

        self._forecast24 =  jsonObj
        _LOGGER.debug("End of 24 forecast udate")

    def get_forecast(self):
        _LOGGER.debug("Start update forecast data")
        s = requests.Session()
        #Forcing headers to avoid 500 error when downloading file
        s.headers.update({"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Encoding":"gzip, deflate, sdch",'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/1337 Safari/537.36'})

        jsonUrl = JSON_FORECAST_URL.format(self._postCode)
        jsonData = s.get(jsonUrl,timeout=10)
        jsonDataTxt = jsonData.text

        jsonObj = json.loads(jsonDataTxt)

        self._forecast =  jsonObj
        _LOGGER.debug("End of forecast udate")

    def get_current_condition(self):
        _LOGGER.debug("Update current condition")

        data = pd.read_csv(CURRENT_CONDITION_URL,sep=';',header=0)

        _LOGGER.debug("Get current condition for : %s"%self._station)
        stationData = data.loc[data['Station/Location'].str.contains(self._station)]
        stationData = stationData.to_dict('records')
        self._condition =  stationData

    def update(self):
        self.get_forecast()
        self.get_current_condition()

    def __get_all_stations(self):
        _LOGGER.debug("Getting all stations from : %s"%(STATION_URL))
        data = pd.read_csv(STATION_URL, sep=';', header=0, skipfooter=4, encoding='latin1', engine='python')
        stationList = {}
        for index, line in data.iterrows():
            stationData = {}
            stationData["code"] = line['Abr.']
            stationData["name"] = line['Station']
            stationData["lat"] = line['Latitude']
            stationData["lon"] = line['Longitude']
            stationData["altitude"] = line["Altitude station m s. mer"]
            stationList[stationData["code"]] = stationData
        return stationList


    def get_closest_station(self,currentLat,currnetLon):
        if(self._allStations is None):
           self._allStations  = self.__get_all_stations()
        hPoint = geopy.Point(currentLat,currnetLon)
        data =[]
        for station in self._allStations:
            sPoint =geopy.Point("%s/%s" % (self._allStations[station]["lat"], self._allStations[station]["lon"]))
            distance = geopy.distance.distance(hPoint,sPoint)
            data += (distance.km,station),
            data.sort(key=lambda tup: tup[0])
        try:
            return data[0][1]
        except:
            _LOGGER.warning("Unable to get closest station for lat : %s lon : %s"%(currentLat,currnetLon))
            return None

    def get_station_name(self,stationId):
        if(self._allStations is None):
           self._allStations  = self.__get_all_stations()

        try:
            return self._allStations[stationId]['name']
        except:
            _LOGGER.warning("Unable to find station name for : %s"%(stationId))
            return None

    def getPostCode(self,lat,lon):
        s = requests.Session()
        lat=str(lat)
        lon=str(lon)
        s.headers.update({"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Encoding":"gzip, deflate, sdch",'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/1337 Safari/537.36'})
        geoData= s.get("https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat="+lat+"&lon="+lon+"&zoom=18").text
        _LOGGER.debug("Got data from opensteetmap: %s"%(geoData))
        geoData = json.loads(geoData)
        try:
            return geoData["address"]["postcode"]
        except:
            _LOGGER.warning("Unable to get post code for location lat : %s lon : %s"%(lat,lon))
            return None

    def get_wind_bearing(self,val):
        lis = {
        "N":[0,11.25],
        "NNE":[11.25,33.75],
        "NE":[33.75,56.25],
        "ENE":[56.25,78.75],
        "E":[78.75,101.25],
        "ESE":[101.25,123.75],
        "SE":[123.75,146.25],
        "SSE":[146.25,168.75],
        "S":[168.75,191.25],
        "SSW":[191.25,213.75],
        "SW":[213.75,236.25],
        "WSW":[236.25,258.75],
        "W":[258.75,281.25],
        "WNW":[281.25,303.75],
        "NW":[303.75,326.25],
        "NNW":[326.25,348.75],
        }

        for it in lis:
            if( lis[it][0] <= float(val) <= lis[it][1]):
                return it
        return "N"


