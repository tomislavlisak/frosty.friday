USE ROLE ACCOUNTADMIN;

USE DATABASE FROSTY_FRIDAY;
USE SCHEMA FF_W128;


CREATE OR REPLACE NETWORK RULE NR_ALLOW_MEDIUM
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('medium.com');

/* 
Create external access integration
!!!!! External access is not supported for trial accounts.
*/

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION EAI_MEDIUM
ALLOWED_NETWORK_RULES = (NR_ALLOW_MEDIUM)
ENABLED = TRUE;


--Snowflake UDTF to scrape Sperheroes

CREATE OR REPLACE FUNCTION UDTF_SCRAPE_SUPERHEROES()
RETURNS TABLE (REGION STRING, NAME STRING, TITLE STRING, COMPANY STRING)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
HANDLER = 'Superheroes'
EXTERNAL_ACCESS_INTEGRATIONS = (EAI_MEDIUM)
PACKAGES = ('requests', 'beautifulsoup4')
AS
$$
import requests
from bs4 import BeautifulSoup

class Superheroes:
    def process(self):
        url = "https://medium.com/snowflake/celebrating-the-2025-data-superheroes-the-innovators-trailblazers-and-changemakers-8e219e48587a"
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.text

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        headers = soup.find_all(lambda tag: tag.name in ["h1", "h2", "h3"] and tag.get_text().strip() in ["Americas", "APJ", "EMEA"])

        for i, header in enumerate(headers):
            current_region = header.get_text().strip()
            next_header = headers[i + 1] if i + 1 < len(headers) else None

            content = []
            for tag in header.find_all_next("p", class_="pw-post-body-paragraph"):
                if next_header and tag.find_previous(lambda t: t == next_header):
                    break

                for element in tag.contents:
                    line = element.text.strip()
                    if not line:
                        continue
                    if line.startswith(','):
                        content[-1] += line  # Merge with the previous line
                    else:
                        content.append(line)

            # Split each line into NAME, TITLE, and COMPANY
            split_content = [line.split(',', maxsplit=2) for line in content]
            for row in split_content:
                if len(row) == 3:
                    yield (current_region, row[0].strip(), row[1].strip(), row[2].strip())
$$;

--Fetch data from Medium article :)
SELECT * FROM TABLE(UDTF_SCRAPE_SUPERHEROES());
