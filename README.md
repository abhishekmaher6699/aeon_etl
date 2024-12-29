# End-to-End Aeon Essay Recommendation System

## ETL Pipeline

## Overview
This project implements an end-to-end recommendation system for Aeon essays. The workflow begins with an **ETL Pipeline** designed to scrape essays and metadata from [Aeon Essays](https://aeon.co/essays), followed by storing the processed data in a **PostgreSQL database** hosted on **AWS RDS**. The data is then utilized to power the recommendation engine.

---

## ETL Pipeline
The ETL (Extract, Transform, Load) pipeline is orchestrated using **Apache Airflow** and consists of three main steps:

### Step 1: Scraping Links
- **Initial Check:** The pipeline checks if the database table is empty.
  - If the table is empty, it scrapes **all article links** available on the Aeon Essays website using **Selenium**.
  - If the table is not empty, it scrapes only the **new links** that are not already present in the database.
- **Output:** The scraped links are saved in a `links.csv` file for further processing.

### Step 2: Scraping Articles
- **Input:** The `links.csv` file is read to retrieve all the links.
- **Processing:** The articles are scraped asynchronously using libraries like **Asyncio** and **BeautifulSoup** to extract content and metadata.
- **Output:** The scraped data is saved in a `scraped_data.csv` file for the loading stage.

### Step 3: Loading Data to Database
- **Input:** The `scraped_data.csv` file is read.
- **Processing:** The data is loaded into the PostgreSQL database hosted on AWS RDS in batches to optimize performance and handle large datasets.

---

## Additional Features

### Real-Time Notifications
- **Trigger Mechanism:**
  - When a new record is added to the database, a function is triggered to send notifications.
- **Notification Pipeline:**
  - The notification is sent to a **Python script** responsible for triggering the **CI/CD pipeline** to build or update the backend of the application.

---
### Future Enhancements

- **Cloud Deployment:** The pipeline can be deployed on a cloud platform to automate the process at regular intervals without any manual interventions.
- **Multiple Site Support:** Right now, we are only scraping articles from AEON, however we can also fetch articles from other websites with more modifications. I will leave that for the future me.
- 
---
## Technical Stack
### Tools and Technologies
- **Scraping:** Selenium, BeautifulSoup, Asyncio
- **Pipeline Orchestration:** Apache Airflow
- **Database:** PostgreSQL on AWS RDS
- **Notification and Automation:** Python, GitHub Actions
- **Deployment:** AWS, Docker, Docker Compose

---

This comprehensive pipeline not only extracts and processes Aeon essays but also provides a robust foundation for building advanced recommendation systems and analytics applications.

