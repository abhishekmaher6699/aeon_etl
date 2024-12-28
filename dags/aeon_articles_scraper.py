import pandas as pd
import aiohttp
from aiohttp import ClientTimeout
import asyncio
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
import random

base_url = "https://aeon.co"
MAX_RETRIES = 5 
RETRY_DELAY = 5 

async def scrape_data(session, url, retries=MAX_RETRIES):
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    content = ''.join([para.text for para in soup.find(id="article-content").find_all("p")]) if soup.find(id="article-content") else "Content not found"
                    title = soup.find(class_="mt-2.5 mb-6 font-semibold font-serif text-7xl leading-none max-[767px]:text-left max-[767px]:text-black max-[960px]:text-[42px] min-[768px]:mt-11 print:text-4xl")
                    title = title.text if title else "Title not found"
                    headline = soup.find(class_="sc-d0d42ecb-4 bXNXzL").text if soup.find(class_="sc-d0d42ecb-4 bXNXzL") else "Headline not found"
                    date = soup.find(class_="sc-2f963901-17 kSvvwV").select('div')[1].text if soup.find(class_="sc-2f963901-17 kSvvwV") else "Date not found"
                    tags = [a.text for a in soup.find(class_="sc-2f963901-13 ezPZgh").select('a')] if soup.find(class_="sc-2f963901-13 ezPZgh") else []
                    image = soup.find('img')['src']
                    return {"url": url, "title": title, "content": content, "tags": tags, "date": date, 'image': image, "headline": headline}
                else:
                    print(f"Failed to retrieve {url} (Status: {response.status})")
                    return {"url": url, "title": f"Failed to retrieve (Status: {response.status})"}

        except Exception as e:
            print(f"Error scraping {url} on attempt {attempt + 1}: {e}")
            attempt += 1
            if attempt < retries:
                # Wait before retrying
                delay = random.uniform(1, 3) * RETRY_DELAY 
                print(f"Retrying {url} in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
            else:
                print(f"Max retries reached for {url}. Error: {e}")
                return {"url": url, "title": "Error", "content": f"Failed after {retries} attempts. Error: {e}"}

def read_links_from_csv(csv_file):
    df = pd.read_csv(csv_file)
    return df['Links'].tolist()

async def scrape_all_links(csv_file):
    links = read_links_from_csv(csv_file)
    
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=120)) as session:
        tasks = []
        for link in links:
            url = base_url + link if link.startswith('/') else link
            # print(f"Scraping URL: {url}")
            tasks.append(scrape_data(session, url))
        
        results = []
        for result in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping pages"):
            results.append(await result)
        
        return results

def save_to_csv(results, output_file):
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False, encoding='utf-8')

async def scrape_articles():
    csv_file = 'links.csv'
    output_file = 'scraped_data.csv' 
    
    scraped_data = await scrape_all_links(csv_file)
    
    save_to_csv(scraped_data, output_file)
    
    print(f"Scraping completed. Data saved to {output_file}.")

if __name__ == '__main__':
    asyncio.run(scrape_articles())