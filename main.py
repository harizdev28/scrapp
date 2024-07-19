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

class EconomicData(Base):
    __tablename__ = 'economic_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    commitments = Column(String)
    date = Column(Date)
    non_commercial_long = Column(Integer)
    non_commercial_short = Column(Integer)
    non_commercial_spreads = Column(Integer)
    non_commercial_changes_long = Column(Integer)
    non_commercial_changes_short = Column(Integer)
    non_commercial_changes_spreads = Column(Integer)
    non_commercial_percent_open_interest_long = Column(String(10))
    non_commercial_percent_open_interest_short = Column(String(10))
    non_commercial_percent_open_interest_spreads = Column(String(10))
    non_commercial_number_of_traders_long = Column(Integer)
    non_commercial_number_of_traders_short = Column(Integer)
    non_commercial_number_of_traders_spreads = Column(Integer)
    commercial_long = Column(Integer)
    commercial_short = Column(Integer)
    commercial_changes_long = Column(Integer)
    commercial_changes_short = Column(Integer)
    commercial_percent_open_interest_long = Column(String(10))
    commercial_percent_open_interest_short = Column(String(10))
    commercial_number_of_traders_long = Column(Integer)
    commercial_number_of_traders_short = Column(Integer)
    total_long = Column(Integer)
    total_short = Column(Integer)
    total_changes_long = Column(Integer)
    total_changes_short = Column(Integer)
    total_percent_open_interest_long = Column(String(10))
    total_percent_open_interest_short = Column(String(10))
    total_number_of_traders = Column(Integer)
    non_reportable_long = Column(Integer)
    non_reportable_short = Column(Integer)
    non_reportable_changes_long = Column(Integer)
    non_reportable_changes_short = Column(Integer)
    non_reportable_percent_open_interest_long = Column(String(10))
    non_reportable_percent_open_interest_short = Column(String(10))
    created_at = Column(DateTime, default=datetime.utcnow)


def scrapp_automated():
    countries = [
        'euro-area', 'united-kingdom', 'canada', 'australia', 'new-zealand', 
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
    #Cot
    cot_cron()

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

def insert_data_economic_if_not_exist(economic_data):
    existing_cot = session.query(EconomicData).filter(
        EconomicData.date == economic_data.date,
        EconomicData.commitments == economic_data.commitments
    ).first()

    if not existing_cot:
        # Insert new indicator if not exists
        session.add(economic_data)
        session.commit()
        logging.info(f"Inserted new cot for {economic_data.commitments} - {economic_data.date}")
    else:
        logging.info(f"Cot for {economic_data.commitments} - {economic_data.date} already exists")

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

@app.route('/api/cot-cron')
def cot_cron():
    paths = [
        '090741', '092741', '096742', '099741', '232741', 
        '097741', '112741', '098662', '088691', '133741'
    ]

    url = "https://tradingster.com/cot/legacy-futures"
    parent = "div"
    child = "table"
    selector = "table-responsive"

    for path in paths:
        crls = Crls(f'{url}/{path}')
        json_data = crls.cot(parent, child, selector)
        #print(json_data)
        economic_data = EconomicData(
            commitments = json_data['commitments'],
            date = json_data['date'],
            non_commercial_long= json_data['non_commercial']['long'],
            non_commercial_short= json_data['non_commercial']['short'],
            non_commercial_spreads= json_data['non_commercial']['spreads'],
            non_commercial_changes_long= json_data['non_commercial']['changes']['long'],
            non_commercial_changes_short= json_data['non_commercial']['changes']['short'],
            non_commercial_changes_spreads= json_data['non_commercial']['changes']['spreads'],
            non_commercial_percent_open_interest_long= json_data['non_commercial']['percent_open_interest']['long'],
            non_commercial_percent_open_interest_short= json_data['non_commercial']['percent_open_interest']['short'],
            non_commercial_percent_open_interest_spreads= json_data['non_commercial']['percent_open_interest']['spreads'],
            non_commercial_number_of_traders_long= json_data['non_commercial']['number_of_traders']['long'],
            non_commercial_number_of_traders_short= json_data['non_commercial']['number_of_traders']['short'],
            non_commercial_number_of_traders_spreads= json_data['non_commercial']['number_of_traders']['spreads'],
            commercial_long= json_data['commercial']['long'],
            commercial_short= json_data['commercial']['short'],
            commercial_changes_long= json_data['commercial']['changes']['long'],
            commercial_changes_short= json_data['commercial']['changes']['short'],
            commercial_percent_open_interest_long= json_data['commercial']['percent_open_interest']['long'],
            commercial_percent_open_interest_short= json_data['commercial']['percent_open_interest']['short'],
            commercial_number_of_traders_long= json_data['commercial']['number_of_traders']['long'],
            commercial_number_of_traders_short= json_data['commercial']['number_of_traders']['short'],
            total_long= json_data['total']['long'],
            total_short= json_data['total']['short'],
            total_changes_long= json_data['total']['changes']['long'],
            total_changes_short= json_data['total']['changes']['short'],
            total_percent_open_interest_long= json_data['total']['percent_open_interest']['long'],
            total_percent_open_interest_short= json_data['total']['percent_open_interest']['short'],
            total_number_of_traders= json_data['total']['number_of_traders'],
            non_reportable_long= json_data['non_reportable']['long'],
            non_reportable_short= json_data['non_reportable']['short'],
            non_reportable_changes_long= json_data['non_reportable']['changes']['long'],
            non_reportable_changes_short= json_data['non_reportable']['changes']['short'],
            non_reportable_percent_open_interest_long= json_data['non_reportable']['percent_open_interest']['long'],
            non_reportable_percent_open_interest_short= json_data['non_reportable']['percent_open_interest']['short'],
            created_at = datetime.now()
        )
        insert_data_economic_if_not_exist(economic_data=economic_data)
    return jsonify({"message": "Successfully executed"}), 200


    
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
    scheduler.add_job(scrapp_automated, 'cron', hour=1, minute=14)
    scheduler.start()

    try:
        print("Starting Flask app with scheduler...")
        app.run(debug=True)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Scheduler stopped.")
