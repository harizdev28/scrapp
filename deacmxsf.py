import json
import requests
from bs4 import BeautifulSoup

def get_deacmxsf():
    url = 'https://www.cftc.gov/dea/futures/deacmxsf.htm'

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        pre_tag = soup.find('pre')
        if pre_tag:
            data = pre_tag.text
            
            lines = data.split('\n')

            gold_section = []
            capture = False
            
            for line in lines:
                if 'GOLD - COMMODITY EXCHANGE INC.' in line:
                    capture = True

                if 'COBALT - COMMODITY EXCHANGE INC.' in line:
                    break

                if 'MICRO GOLD - COMMODITY EXCHANGE INC.' in line:
                    break

                if capture:
                    gold_section.append(line)
            
            # Extract relevant lines from the gold section
            # Extract relevant lines from the gold section
            commitments_line = []
            changes_line = []
            
            for i, line in enumerate(gold_section):
                if 'COMMITMENTS' in line:
                    commitments_line = gold_section[i+1].strip().split()
                if 'CHANGES FROM' in line:
                    changes_line = gold_section[i+1].strip().split()
            
            # Organize the extracted data
            non_commercial_commitments = commitments_line[:3]
            commercial_commitments = commitments_line[3:5]
            
            non_commercial_changes = changes_line[:3]
            commercial_changes = changes_line[3:5]

            # Organize the extracted data
            data_json = {
                "non_commercial_commitments": {
                    "long": commitments_line[0],
                    "short": commitments_line[1],
                    "spreads": commitments_line[2]
                },
                "commercial_commitments": {
                    "long": commitments_line[3],
                    "short": commitments_line[4]
                },
                "non_commercial_changes": {
                    "long": changes_line[0],
                    "short": changes_line[1],
                    "spreads": changes_line[2]
                },
                "commercial_changes": {
                    "long": changes_line[3],
                    "short": changes_line[4]
                }
            }
            
            # Print organized data in JSON format
            return (json.dumps(data_json, indent=4))

        else:
            return ('No preformatted text block found')
    else:
        return (f'Failed to retrieve the page. Status code: {response.status_code}')
