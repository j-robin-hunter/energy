import requests
from bs4 import BeautifulSoup

page = requests.get("http://www.goodwe-power.com/PowerStationPlatform/PowerStationReport/HybridInventer?ID=0ea46d08-f1f1-42af-a99d-c96a29d8d054&InventerType=HybridInvter&HaveAdverseCurrentData=0&HaveEnvironmentData=0")
soup = BeautifulSoup(page.content, 'html.parser')
values_in = soup.find("ul", {"class": "unit clearfix"})
find = values_in.find("p", {"id": "PowerStationPac"})
print(find)
find = values_in.find("p", {"id": "PowerStationEDay"})
print(find)
find = values_in.find("p", {"id": "PowerStationETotal"})
print(find)
find = values_in.find("p", {"id": "PowerStationIncome"})
print(find)
find = values_in.find("p", {"id": "PowerStationTotalPlant"})
print(find)
find = values_in.find("p", {"id": "PowerStationCO2"})
print(find)
