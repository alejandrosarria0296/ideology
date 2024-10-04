import os
import time
from selenium import webdriver
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

def get_docs(driver, download_folder):
    buttons = driver.find_elements(By.XPATH, "//button[@title='Descargar Pdf']")
    labels = [label.text for label in driver.find_elements(By.XPATH, "//label[contains(@id, 'j_idt12')]")]
    for i, button in enumerate(buttons):
        try:
            file_name = f"gaceta_{labels[i]}.pdf"
            if not os.path.exists(os.path.join(download_folder, file_name)):
                button.click()
                time.sleep(2)
        except:
            pass

def wait_for_downloads(download_folder, timeout=30):
    """
    Wait for all files in the download directory to finish downloading.
    This function will wait until no `.crdownload` files exist.
    """
    end_time = time.time() + timeout
    while True:
        # Check if there are any temporary .crdownload files
        if any(filename.endswith('.crdownload') for filename in os.listdir(download_folder)):
            # If .crdownload files are found, wait for a bit and check again
            time.sleep(3)
        else:
            break
        if time.time() > end_time:
            print("Download timed out.")
            break

def get_next_page(driver):
    next_button = driver.find_element(By.CLASS_NAME, "ui-paginator-next")
    next_button.click()
    time.sleep(5)

def is_last_page(driver):
    try:
        driver.find_element(By.XPATH, "//span[contains(@class, 'ui-paginator-next') and contains(@class, 'ui-state-disabled')]")
        return True
    except NoSuchElementException:
        return False

if __name__ == '__main__':
    base_url = r'http://svrpubindc.imprenta.gov.co/senado/'
    download_path = r"C:\Users\asarr\Documents\MACSS\Thesis\data"
    driver = setup_driver(download_path, base_url)

    while True:
        get_docs(driver, download_path)
        wait_for_downloads(download_path, 600)
        if is_last_page(driver):
            break
        get_next_page(driver)

    # driver.quit()
