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
            return None, None
        
        page_content = response.text
        soup = bs4(page_content, "html.parser")
        parent_element = soup.find(parent_tag, attrs=attrs)
        
        if not parent_element:
            return None, None
        
        child_element = parent_element.find(child_tag)
        
        if not child_element:
            return None, None

        # Extracting table headers
        headers = []
        for th in child_element.find_all("th"):
            headers.append(th.text.strip())

        # Extracting table rows
        table_data = []
        for tr in child_element.find_all("tr")[1:]:
            td_tags = tr.find_all("td")
            td_values = [td.text.strip() for td in td_tags]
            table_data.append(td_values)

        return headers, table_data

    def to_json(self, headers, data):
        # Create a list of dictionaries
        table_json = []
        for row in data:
            row_dict = {}
            for i in range(len(row)):
                # Only add to the dictionary if headers and row lengths match
                if i < len(headers):
                    row_dict[headers[i]] = row[i]
            table_json.append(row_dict)
        
        return table_json
