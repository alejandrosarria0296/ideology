import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import numpy as np
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

def setup_driver(download_folder):
    # Set Chrome options to download files automatically
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_folder,  # Default download directory
        "plugins.always_open_pdf_externally": True,  # Disable PDF viewer
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Set up the driver with ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver

base_url = r'http://svrpubindc.imprenta.gov.co/senado/'
download_path = r"C:\Users\asarr\Documents\MACSS\Thesis\data"

def get_docs(driver):
    buttons = driver.find_elements(By.XPATH, "//button[@title='Descargar Pdf']")
    for button in buttons:
        try:
            button.click()
            time.sleep(2)
        except:
            pass

if __name__ == '__main__':
    driver = setup_driver(download_path)
    driver.get(base_url)
    get_docs(driver)
    driver.quit()

