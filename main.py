import json
import logging
from flask import Flask, request, jsonify
from scrap import Crls
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = Flask(__name__)


# Setup logging
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Database setup
DATABASE_URI = 'mysql+pymysql://root:@localhost/db_naked_fx'
engine = create_engine(DATABASE_URI)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

class EconomicIndicator(Base):
    __tablename__ = 'economic_indicator'

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    country = Column(String(100))
    indicator_type = Column(String(50))
    indicator = Column(String(100))
    last_value = Column(Float)
    previous_value = Column(Float)
    highest_value = Column(Float)
    lowest_value = Column(Float)
    unit = Column(String(50))
    frequency = Column(CHAR(6))
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

def scrape_indicators():
    countries = [
        'euro-area', 'united-kingdom', 'canada', 'australia', 'new-zaeland', 
        'japan', 'switzerland', 'china', 'united-states'
    ]
    for country in countries:
        crls = Crls(f'https://tradingeconomics.com/{country}/indicators')
        indicators_data = crls.indicator()

        # Iterate over each indicator
        for tab_name, tab_data in indicators_data.items():
            # Use the first tab data to determine frequency
            if tab_data:
                frequency = tab_data[0].get('')  # Assume the key "" contains frequency
            else:
                frequency = None

            # Iterate over indicators in the current tab
            for indicator in tab_data:
                try:
                    last_value = float(indicator.get('Last', 0)) if indicator.get('Last', '').strip() else None
                    previous_value = float(indicator.get('Previous', 0)) if indicator.get('Previous', '').strip() else None
                    highest_value = float(indicator.get('Highest', 0)) if indicator.get('Highest', '').strip() else None
                    lowest_value = float(indicator.get('Lowest', 0)) if indicator.get('Lowest', '').strip() else None
                except ValueError as e:
                    print(f"Error converting value to float for {country}: {e}")
                    continue

                econ_ind = EconomicIndicator(
                    date=datetime.now().date(),  # Update date here
                    country=country,
                    indicator_type=tab_name,
                    indicator=indicator.get('Indicator'),
                    last_value=last_value,
                    previous_value=previous_value,
                    highest_value=highest_value,
                    lowest_value=lowest_value,
                    unit=indicator.get('Unit'),
                    frequency=frequency
                )
                insert_indicator_if_not_exists(indicator_data=econ_ind)

def insert_indicator_if_not_exists(indicator_data):
    # Check if indicator with the same combination of country, indicator_type, and indicator exists
    existing_indicator = session.query(EconomicIndicator).filter(
        EconomicIndicator.country == indicator_data.country,
        EconomicIndicator.indicator_type == indicator_data.indicator_type,
        EconomicIndicator.indicator == indicator_data.indicator,
        EconomicIndicator.frequency == indicator_data.frequency
    ).first()

    if not existing_indicator:
        # Insert new indicator if not exists
        session.add(indicator_data)
        session.commit()
        logging.info(f"Inserted new indicator for {indicator_data.country} - {indicator_data.indicator}")
    else:
        logging.info(f"Indicator for {indicator_data.country} - {indicator_data.indicator} already exists")

@app.route('/api/heatmap', methods=['GET'])
def heat_map_endpoint():
    crls = Crls('https://tradingeconomics.com/matrix')
    resp = crls.get_heatmap()
    return jsonify(resp)

@app.route('/api/indicator/<country>')
def indicator_endpoint(country):
    crls = Crls(f'https://tradingeconomics.com/{country}/indicators')
    resp = crls.indicator()
    return jsonify(resp)
    
@app.route('/api/cot', methods=['GET'])
def cot_endpoint():
    url = request.args.get('url')
    parent = request.args.get('parent')
    child = request.args.get('child')
    selector = request.args.get('selector')

    if not all([url, parent, child, selector]):
        return jsonify({'error': 'Missing required parameters: url, parent, child, selector'}), 400

    crls = Crls(url)
    headers, data = crls.cot(parent, child, selector)

    if headers and data:
        json_data = crls.to_json(headers, data)
        return jsonify(json_data)
    else:
        return jsonify({"message": "Failed to retrieve data."})

if __name__ == '__main__':
    # Setup the scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(scrape_indicators, 'cron', hour=1, minute=14)
    scheduler.start()

    try:
        print("Starting Flask app with scheduler...")
        app.run(debug=True)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Scheduler stopped.")