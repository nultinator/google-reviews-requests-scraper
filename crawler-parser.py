import os
import re
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, locality, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.google.com/maps/search/{formatted_keyword}/@{locality},14z/data=!3m1!4b1?entry=ttu"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
            soup = BeautifulSoup(response.text, "html.parser")
            business_links = soup.select("div div a")
            excluded_words = ["Sign in"]
            for business_link in business_links:
                name = business_link.get("aria-label")
                if not name or name in excluded_words or "Visit" in name:
                    continue
                maps_link = business_link.get("href")
                full_card = business_link.parent
                
                rating_holder = full_card.select_one("span[role='img']")

                rating = 0.0
                rating_count = 0

                if rating_holder:
                    rating_array = rating_holder.text.split("(")
                    rating = rating_array[0]
                    rating_count = int(rating_array[1].replace(")", "").replace(",", ""))
                
                search_data = {
                    "name": name,
                    "stars": rating,
                    "url": maps_link,
                    "rating_count": rating_count
                }
                   
                print(search_data)
                
            success = True
            logger.info(f"Successfully parsed data from: {url}")
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries += 1
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")




def start_scrape(keyword, location, localities, retries=3):
    for locality in localities:
        scrape_search_results(keyword, location, locality, retries=retries)
        

if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    
    LOCATION = "us"
    LOCALITIES = ["42.3,-83.5","42.35,-83.5", "42.4,-83.5"]

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["restaurant"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        start_scrape(keyword, LOCATION, LOCALITIES, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")