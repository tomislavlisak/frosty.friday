import requests
from bs4 import BeautifulSoup
import pandas as pd
from snowflake.snowpark import Session


# URL to scrape
url = "https://medium.com/snowflake/celebrating-the-2025-data-superheroes-the-innovators-trailblazers-and-changemakers-8e219e48587a"
response = requests.get(url)
response.raise_for_status()
html_content = response.text

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

# List of region headers to search for
regions = ["Americas", "APJ", "EMEA"]
content = []

# Loop through each region to extract heroes
for region in regions:
    start_section = soup.find(lambda tag: tag.name in ["h1", "h2", "h3"] and region in tag.text)
    if not start_section:
        continue

    merged_line = ""
    for tag in start_section.find_all_next("p", class_="pw-post-body-paragraph"):
        for element in tag.contents:
            line = element.text.strip()
            if not line:
                continue
            if line.startswith(','):
                merged_line += line  # Merge consecutive lines
            else:
                if merged_line:  # If there's a previously merged line, add it to data
                    parts = merged_line.split(',', maxsplit=2)
                    if len(parts) == 3:
                        content.append([region] + parts)
                merged_line = line  # Start a new merged line

    # Add the last merged line if present
    if merged_line:
        parts = merged_line.split(',', maxsplit=2)
        if len(parts) == 3:
            content.append([region] + parts)

# Convert to DataFrame
df = pd.DataFrame(content, columns=["REGION", "NAME", "TITLE", "COMPANY"])
print(df)

#Connect to Snowflake and save the DataFrame (if needed)
session = Session.builder.config("connection_name", "frosty").create()
session.write_pandas(database='FROSTY_FRIDAY', schema='FF_W128', table_name='SUPERHEROES_2025', df=df, auto_create_table=True, overwrite=True)
