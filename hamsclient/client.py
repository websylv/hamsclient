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
MS_SEARCH_URL = 'https://www.meteosuisse.admin.ch/home/actualite/infos.html?ort={}&pageIndex=0&tab=search_tab'
CURRENT_CONDITION_URL= 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv'
STATION_URL = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/info/VQHA80_fr.txt"
MS_24FORECAST_URL = "https://www.meteosuisse.admin.ch/product/output/forecast-chart/{}/fr/{}00.json"
MS_24FORECAST_REF = "https://www.meteosuisse.admin.ch//content/meteoswiss/fr/home.mobile.meteo-products--overview.html"

class meteoSwissClient():
    def __init__(self,displayName=None,postcode=None,station=None):
        _LOGGER.debug("MS Client INIT")
        self._postCode = postcode
        self._station = station
        self._name = displayName
        self._allStations = None
        _LOGGER.debug("INIT meteoswiss client : name = %s station = %s postcode = %s"%(self._name,self._station,self._postCode))
        
    
    def get_data(self):
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
        searchUrl = MS_SEARCH_URL.format(self._postCode)
        _LOGGER.debug("Main URL : %s"%searchUrl)
        tmpSearch = s.get(searchUrl,timeout=10)

        soup = BeautifulSoup(tmpSearch.text,features="html.parser")
        widgetHtml = soup.find_all("section",{"id": "weather-widget"})
        jsonUrl = widgetHtml[0].get("data-json-url")
        jsonDataFile = str.split(jsonUrl,'/')[-1]
        newJsonDataFile = str(self._postCode)+"00.json"
        jsonUrl = str(jsonUrl).replace(jsonDataFile,newJsonDataFile)
        dataUrl = MS_BASE_URL + jsonUrl
        _LOGGER.debug("Data URL : %s"%dataUrl)
        s.headers.update({'referer': searchUrl})
        jsonData = s.get(dataUrl,timeout=10)
        jsonDataTxt = jsonData.text

        jsonObj = json.loads(jsonDataTxt)

        self._forecast =  jsonObj
        _LOGGER.debug("End of forecast udate")

    def get_current_condition(self):
        _LOGGER.debug("Update current condition")
        data = pd.read_csv(CURRENT_CONDITION_URL,sep=';',header=1)
        _LOGGER.debug("Get current condition for : %s"%self._station)
        stationData = data.loc[data['stn'].str.contains(self._station)]
        stationData = stationData.to_dict('records')
        self._condition =  stationData
    
    def update(self):
        self.get_forecast()
        self.get_current_condition()

    def __get_all_stations(self):
        s = requests.Session()
        _LOGGER.debug("Getting all stations from : %s"%(STATION_URL))
        tmp = s.get(STATION_URL)
        descriptionLines = tmp.text.split('\n')
        cordinatesFound = False
        stationList = {}
        for line in descriptionLines:
            if not cordinatesFound :
                if(re.match(r"Stations\sCoordinates", line)):
                    cordinatesFound = True
            else:
                try:
                    if(re.match(r"^[A-Z]{3}\s+",line)):
                        lineParts = None
                        lineParts = re.split(r'\s\s+',line)
                        
                        ## Saving station data to a dictionnary
                        stationData = {}
                        stationData["code"] = lineParts[0]
                        stationData["name"] = lineParts[1]
                        stationData["lat"] = lineParts[3].split("/")[1]
                        stationData["lon"] = lineParts[3].split("/")[0]
                        stationData["coordianteKM"] = lineParts[4]
                        stationData["altitude"] = lineParts[5].strip()
                        
                        stationList[lineParts[0]] = stationData
                except:
                    pass    
        return stationList 


    def get_closest_station(self,currentLat,currnetLon):
        if(self._allStations is None):
           self._allStations  = self.__get_all_stations()
        hPoint = geopy.Point(currentLat,currnetLon)
        data =[]
        for station in self._allStations:
            sPoint =geopy.Point(self._allStations[station]["lat"]+"/"+self._allStations[station]["lon"])
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

                