import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException

def setup_driver(download_folder, url):
    # Set Chrome options to download files automatically
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_folder,  # Default download directory
        "plugins.always_open_pdf_externally": True,  # Disable PDF viewer
        "safebrowsing.enabled": "false", 
        "safebrowsing.disable_download_protection": True 
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Add arguments to allow insecure downloads
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--safebrowsing-disable-download-protection')  # Disable blocking of insecure downloads
    
    # New arguments to allow mixed content and disable SSL certificate validation
    chrome_options.add_argument('--allow-running-insecure-content')  # Allow loading insecure content
    chrome_options.add_argument('--ignore-certificate-errors')  # Ignore SSL certificate errors
    chrome_options.add_argument('--disable-web-security')  # Disable web security features
    chrome_options.add_argument(f"--unsafely-treat-insecure-origin-as-secure={url}")
    
    # Set up the driver with ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(url)
    input_element = driver.find_element(By.ID, "formResumen:dataTableResumen:j_idt20:filter")
    input_element.send_keys("Acta")

    return driver

def create_info_dict(driver, table_selector="table[id*='formResumen:dataTableResumen']"):
    data_dict = {}
    rows = driver.find_elements(By.CSS_SELECTOR, f'{table_selector} tr')

    for row in rows:
        try:
            id_value = row.find_element(By.CSS_SELECTOR, 'label[id*="j_idt12"]').text
            chamber = row.find_element(By.CSS_SELECTOR, 'label[id*="j_idt17"]').text
            date = row.find_element(By.CSS_SELECTOR, 'label[id*="j_idt19"]').text
            data_dict[id_value] = (date, chamber)
        except Exception as e:
            print(f"Skipping row due to error: {e}")
    return data_dict

def get_next_page(driver):
    try:
        # Wait for the overlay to disappear
        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element((By.ID, "dialogStatus_modal"))
        )
        
        next_button = driver.find_element(By.CSS_SELECTOR, ".ui-paginator-next")
        next_button.click()
    except Exception as e:
        print(f"Error navigating to the next page: {e}")

def is_last_page(driver):
    try:
        driver.find_element(By.XPATH, "//span[contains(@class, 'ui-paginator-next') and contains(@class, 'ui-state-disabled')]")
        return True
    except NoSuchElementException:
        return False

if __name__ == "__main__":
    download_folder = os.path.join(os.getcwd(), "downloads")
    url = "http://svrpubindc.imprenta.gov.co/senado/"
    driver = setup_driver(download_folder, url)
    master_dict = create_info_dict(driver)
    while not is_last_page(driver):
        get_next_page(driver)
        master_dict.update(create_info_dict(driver))
        time.sleep(5)
    driver.quit()

    df = pd.DataFrame(master_dict).T
    df.to_csv(r"C:\Users\asarr\Documents\MACSS\Thesis\results\session_info.csv", index=False)



