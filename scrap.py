import requests
from bs4 import BeautifulSoup as bs4
from fake_useragent import UserAgent

class Crls:
    def __init__(self, link):
        self.link = link

    def get_heatmap(self):
        # Fetch the content from the URL
        ua = UserAgent()
        ua_rand = ua.random
        headers = {'User-Agent': ua_rand}
        response = requests.get(self.link, headers=headers)
        if response.status_code != 200:
            return {"error": "Failed to fetch the URL"}

        # Parse the HTML content
        soup = bs4(response.content, 'html.parser')
        table = soup.find('table', id='matrix')

        # Extract table headers
        headers = [th.get_text(strip=True) for th in table.find_all('th')]

        # Extract table rows
        rows = []
        for tr in table.find_all('tr')[1:]:  # Skip the header row
            cells = [td.get_text(strip=True) for td in tr.find_all('td')]
            rows.append(dict(zip(headers, cells)))
        
        return rows

    def indicator(self):
        ua = UserAgent()
        ua_rand = ua.random
        headers = {'User-Agent': ua_rand}
        response = requests.get(self.link, headers=headers)

        if response.status_code != 200:
            return None, None
        
        page_content = response.text
        soup = bs4(page_content, "html.parser")
        # Find all the tab names
        tab_elements = soup.select('ul.nav.nav-tabs li a')
        tab_names = [tab.text.strip() for tab in tab_elements]

        # Create a dictionary to store data from each tab
        tabs_data = {}

        # Iterate over each tab
        for tab_name in tab_names:
            # Find the corresponding table
            tab_content = soup.find('div', {'id': tab_name.lower()})
            if not tab_content:
                continue
            table = tab_content.find('table')
            
            if not table:
                continue
            
            # Extract table headers
            headers = ['Indicator'] + ['Unit'] + [header.text.strip() for header in table.find_all('th')[1:]]
            
            # Extract table rows
            rows = []
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                indicator = cells[0].get_text(strip=True)
                unit = cells[5].get_text(strip=True)
                row_data = [indicator] + [unit] + [cell.get_text(strip=True) for cell in cells[1:]]
                rows.append(dict(zip(headers, row_data)))
            
            # Store the data in the dictionary
            tabs_data[tab_name] = rows

        # Convert the dictionary to JSON
        json_data = tabs_data
        return json_data

    def cot(self, parent_tag, child_tag, attrs):
        ua = UserAgent()
        ua_rand = ua.random
        headers = {'User-Agent': ua_rand}
        response = requests.get(self.link, headers=headers)

        if response.status_code != 200:
            return {"error": "Failed to fetch the URL"}
        
        page_content = response.text
        soup = bs4(page_content, "html.parser")

         # Extract data from the webpage using BeautifulSoup
        strong_tag1 = soup.select_one("div:nth-of-type(1) > div:nth-of-type(1) > strong:nth-of-type(1)")
        commitments = strong_tag1.text if strong_tag1 else "N/A"

        strong_tag2 = soup.select_one("div:nth-of-type(1) > div:nth-of-type(2) > div > strong")
        date = strong_tag2.text.split(": ")[1] if strong_tag2 else "N/A"

        parent_element = soup.find(parent_tag, attrs=attrs)
        
        if not parent_element:
            return {"error": "Parent element not found"}
        
        child_element = parent_element.find(child_tag)  # This contains the table

        if not child_element:
            return {"error": "Child element not found"}

        # Extract and return data as JSON
        data = self.extract_data(child_element)
        data['commitments'] = commitments
        data['date'] = date
        return data

    def get_number(self, td):
        text = td.get_text(strip=True)
        if text.startswith('(') and text.endswith(')'):
            return int(text[1:-1].replace(',', ''))
        elif text.startswith('-'):
            return int(text.replace(',', ''))
        elif text.startswith('+'):
            return int(text[1:].replace(',', ''))
        else:
            return int(text.replace(',', ''))

    def get_percent(self, td):
        return td.get_text(strip=True)

    def extract_data(self, child_element):
        data = {}
        rows = child_element.select('tbody tr')

        def safe_get_number(td):
            try:
                return self.get_number(td)
            except (IndexError, AttributeError, ValueError):
                return None

        def safe_get_percent(td):
            try:
                return self.get_percent(td)
            except (IndexError, AttributeError):
                return None

        if len(rows) < 8:
            return {"error": "Unexpected table structure"}

        # Open Interest and Change in Open Interest
        open_interest_row = rows[0].select('td')
        change_interest_row = rows[2].select('td')
        if len(open_interest_row) > 5 and len(change_interest_row) > 5:
            data['open_interest'] = safe_get_number(open_interest_row[5].find('span'))
            data['change_in_open_interest'] = safe_get_number(change_interest_row[5].find('span'))

        # Non-Commercial Data
        non_commercial_row = rows[1].select('td')
        non_commercial_changes_row = rows[3].select('td')
        non_commercial_percent_row = rows[5].select('td')
        non_commercial_traders_row = rows[7].select('td')

        data['non_commercial'] = {
            'long': safe_get_number(non_commercial_row[0]),
            'short': safe_get_number(non_commercial_row[1]),
            'spreads': safe_get_number(non_commercial_row[2]),
            'changes': {
                'long': safe_get_number(non_commercial_changes_row[0]),
                'short': safe_get_number(non_commercial_changes_row[1]),
                'spreads': safe_get_number(non_commercial_changes_row[2])
            },
            'percent_open_interest': {
                'long': safe_get_percent(non_commercial_percent_row[0]),
                'short': safe_get_percent(non_commercial_percent_row[1]),
                'spreads': safe_get_percent(non_commercial_percent_row[2])
            },
            'number_of_traders': {
                'long': safe_get_number(non_commercial_traders_row[0]),
                'short': safe_get_number(non_commercial_traders_row[1]),
                'spreads': safe_get_number(non_commercial_traders_row[2])
            }
        }

        # Commercial Data
        data['commercial'] = {
            'long': safe_get_number(non_commercial_row[3]),
            'short': safe_get_number(non_commercial_row[4]),
            'changes': {
                'long': safe_get_number(non_commercial_changes_row[3]),
                'short': safe_get_number(non_commercial_changes_row[4])
            },
            'percent_open_interest': {
                'long': safe_get_percent(non_commercial_percent_row[3]),
                'short': safe_get_percent(non_commercial_percent_row[4])
            },
            'number_of_traders': {
                'long': safe_get_number(non_commercial_traders_row[3]),
                'short': safe_get_number(non_commercial_traders_row[4])
            }
        }

        # Total Data
        total_row = rows[1].select('td')
        total_changes_row = rows[3].select('td')
        total_percent_row = rows[5].select('td')
        total_traders_row = rows[6].select('td')

        data['total'] = {
            'long': safe_get_number(total_row[5]),
            'short': safe_get_number(total_row[6]),
            'changes': {
                'long': safe_get_number(total_changes_row[5]),
                'short': safe_get_number(total_changes_row[6])
            },
            'percent_open_interest': {
                'long': safe_get_percent(total_percent_row[5]),
                'short': safe_get_percent(total_percent_row[6])
            },
            'number_of_traders': safe_get_number(total_traders_row[1].find('span'))
        }

        # Non-Reportable Data
        data['non_reportable'] = {
            'long': safe_get_number(non_commercial_row[7]),
            'short': safe_get_number(non_commercial_row[8]),
            'changes': {
                'long': safe_get_number(non_commercial_changes_row[7]),
                'short': safe_get_number(non_commercial_changes_row[8])
            },
            'percent_open_interest': {
                'long': safe_get_percent(non_commercial_percent_row[7]),
                'short': safe_get_percent(non_commercial_percent_row[8])
            }
        }

        return data
