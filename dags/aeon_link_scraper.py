import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

from database_operations import start_connection, close_connection, url_exists_in_db

def setup_driver():

    selenium_url = "http://selenium:4444/wd/hub"

    options = Options()
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.85 Safari/537.36"
    )
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Remote(
    command_executor=selenium_url,
    options=options
    )

    return driver

def scrape_initial_links():

    url = "https://aeon.co/essays"

    driver = setup_driver()
    driver.get(url)

    wait = WebDriverWait(driver, 10)

    click_count = 0
    all_articles = set()

    with tqdm(total=2500, desc="Collecting Links") as progress_bar:
        while True:
            try:
                more_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='MORE']")))
                
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", more_button)
                time.sleep(0.5) 

                try:
                    more_button.click()
                except ElementClickInterceptedException:
                    driver.execute_script("arguments[0].click();", more_button)

                click_count += 1

                driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 200);")
                time.sleep(2)
                
                soup = BeautifulSoup(driver.page_source, "html.parser")
                articles = soup.find_all('a', href=True)
                new_articles = {a['href'] for a in articles}
                added_articles = len(new_articles - all_articles)
                all_articles.update(new_articles)

                progress_bar.update(added_articles)
                progress_bar.set_postfix({'Unique Links': len(all_articles)})
            
                
            except Exception as e:
                print(f"No more 'MORE' button to click or an error occurred: {e}")
                break

        print(f"Final total articles found: {len(all_articles)}")

        # if not os.path.exists('artifacts'):
        #     os.makedirs('artifacts')
        
        essays = [link for link in all_articles if 'essay' in link]
        pd.DataFrame(essays, columns=['Links']).to_csv('links.csv', index=False)

        driver.quit()

def scrape_new_links():

    url = "https://aeon.co/essays"

    conn, cursor = start_connection()

    driver = setup_driver()
    driver.get(url)

    wait = WebDriverWait(driver, 10)

    click_count = 0
    new_articles_count = 0
    all_articles = set()


    terminate = False
    while not terminate:
        try:
            more_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='MORE']")))
            
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", more_button)
            time.sleep(0.5) 

            try:
                more_button.click()
            except ElementClickInterceptedException:
                driver.execute_script("arguments[0].click();", more_button)

            click_count += 1

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 200);")
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            articles = soup.find_all('a', href=True)

            new_articles = [a['href'] for a in articles if 'essays' in a['href']]

            for article in new_articles:
                full_url = "https://aeon.co" + article
                if url_exists_in_db(full_url, cursor):
                    print(f"URL {article} already exists. Terminating process.")
                    terminate = True
                    break
                else:
                    print(article)
                    all_articles.add(article) 
                    new_articles_count += 1
            
            if terminate:
                break

        except Exception as e:
            print(f"No more 'MORE' button to click or an error occurred: {e}")
            break

    close_connection(conn, cursor)

    print(f"Final total articles found: {len(all_articles)}")

    essays = [link for link in all_articles if 'essay' in link]
    pd.DataFrame(essays, columns=['Links']).to_csv('links.csv', index=False)

    driver.quit()