import requests
import json
from bs4 import BeautifulSoup

def main():
 urlJSON = "https://zenodo.org/record/6603766/export/json"
 soup = BeautifulSoup( requests.get(urlJSON).content, "html.parser")
 datasetJSON = json.loads( soup.find("pre", style = "white-space: pre-wrap;").contents[0] )

 datasetURLs = [ "https://zenodo.org/record/6603766/files/{}".format(entity["key"]) for entity in datasetJSON["files"] if "csv.gz" in entity["key"] ]

 with open('datasetURLs.json', 'w') as json_file:
  json.dump(datasetURLs, json_file)

if __name__=="__main__":
 main()
