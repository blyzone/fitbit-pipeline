#!/usr/bin/env python3
"""
Enhanced Fitbit to InfluxDB Data Pipeline - Windows Installation Version
Comprehensive implementation with expanded metrics and proper error handling.
"""

import os
import json
import base64
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

# Try to import InfluxDB client, provide helpful error if missing
try:
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS
except ImportError:
    print("ERROR: InfluxDB client not installed. Run: pip install influxdb-client")
    exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fitbit_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration - Load from environment variables
FITBIT_CLIENT_ID = os.getenv('FITBIT_CLIENT_ID', '')
FITBIT_CLIENT_SECRET = os.getenv('FITBIT_CLIENT_SECRET', '') 
FITBIT_ACCESS_TOKEN = os.getenv('FITBIT_ACCESS_TOKEN', '')
FITBIT_REFRESH_TOKEN = os.getenv('FITBIT_REFRESH_TOKEN', '')

INFLUX_URL = os.getenv('INFLUX_URL', 'http://localhost:8086')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN', '')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'fitbit')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET', 'fitbit')

# Health condition flags for specialized monitoring
HEALTH_CONDITIONS = {
    'long_covid': os.getenv('HEALTH_LONG_COVID', 'true').lower() == 'true',
    'pots': os.getenv('HEALTH_POTS', 'true').lower() == 'true',
    'small_fiber_neuropathy': os.getenv('HEALTH_SMALL_FIBER_NEUROPATHY', 'true').lower() == 'true',
    'pem': os.getenv('HEALTH_PEM', 'true').lower() == 'true',
    'dysautonomia': os.getenv('HEALTH_DYSAUTONOMIA', 'true').lower() == 'true'
}

# Comprehensive metrics configuration
DAILY_METRICS = [
    'activities/steps',
    'activities/calories', 
    'activities/distance',
    'activities/floors',
    'activities/elevation',
    'activities/minutesSedentary',
    'activities/minutesLightlyActive',
    'activities/minutesFairlyActive', 
    'activities/minutesVeryActive',
    'activities/active-zone-minutes',
    'activities/heart',
    'body/weight',
    'body/bmi',
    'body/fat',
    'sleep',
    'spo2',
    'hrv'
]

INTRADAY_METRICS = [
    'activities/heart',
    'activities/steps',
    'activities/calories',
    'activities/distance',
    'activities/floors',
    'activities/active-zone-minutes'
]

class FitbitAPI:
    def __init__(self):
        self.client_id = FITBIT_CLIENT_ID
        self.client_secret = FITBIT_CLIENT_SECRET
        self.access_token = FITBIT_ACCESS_TOKEN
        self.refresh_token = FITBIT_REFRESH_TOKEN
        self.base_url = "https://api.fitbit.com/1/user/-"
        
        # Validate required credentials
        if not all([self.client_id, self.client_secret, self.access_token, self.refresh_token]):
            logger.error("Missing required Fitbit credentials in environment variables")
            raise ValueError("Fitbit credentials not properly configured")
    
    def refresh_access_token(self) -> bool:
        """Refresh Fitbit OAuth2 token"""
        try:
            auth_string = f"{self.client_id}:{self.client_secret}"
            auth_header = base64.b64encode(auth_string.encode()).decode()
            
            headers = {
                'Authorization': f'Basic {auth_header}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token
            }
            
            response = requests.post(
                'https://api.fitbit.com/oauth2/token',
                headers=headers,
                data=data,
                timeout=30
            )
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                self.refresh_token = token_data['refresh_token']
                
                logger.info("Token refreshed successfully")
                return True
            else:
                logger.error(f"Token refresh failed: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return False
    
    def make_request(self, endpoint: str, date: str = None, detail_level: str = None) -> Optional[Dict]:
        """Make authenticated request to Fitbit API with rate limiting"""
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        # Construct URL
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
            
        url = f"{self.base_url}/{endpoint}/date/{date}"
        
        # Add detail level for intraday data
        if detail_level:
            url += f"/{detail_level}"
            
        url += ".json"
        
        try:
            logger.info(f"Making request to: {endpoint}")
            response = requests.get(url, headers=headers, timeout=30)
            
            # Handle token expiration
            if response.status_code == 401:
                logger.info("Access token expired, refreshing...")
                if self.refresh_access_token():
                    headers['Authorization'] = f'Bearer {self.access_token}'
                    response = requests.get(url, headers=headers, timeout=30)
                else:
                    logger.error("Failed to refresh token")
                    return None
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 3600))
                logger.warning(f"Rate limited, waiting {retry_after} seconds")
                time.sleep(retry_after)
                return self.make_request(endpoint, date, detail_level)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {endpoint}: {str(e)}")
            return None

class InfluxDBWriter:
    def __init__(self):
        if not INFLUX_TOKEN:
            logger.error("INFLUX_TOKEN not set in environment variables")
            raise ValueError("InfluxDB token not configured")
            
        try:
            self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            logger.info(f"Connected to InfluxDB at {INFLUX_URL}")
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {str(e)}")
            raise
        
    def write_points(self, points: List[Point]):
        """Write points to InfluxDB"""
        try:
            if points:
                self.write_api.write(bucket=INFLUX_BUCKET, record=points)
                logger.info(f"Wrote {len(points)} points to InfluxDB")
        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {str(e)}")
    
    def close(self):
        self.client.close()

class FitbitDataProcessor:
    def __init__(self, fitbit_api: FitbitAPI, influx_writer: InfluxDBWriter):
        self.fitbit_api = fitbit_api
        self.influx_writer = influx_writer
    
    def process_daily_summary(self, metric: str, date: str) -> List[Point]:
        """Process daily summary data"""
        data = self.fitbit_api.make_request(metric, date)
        if not data:
            return []
        
        points = []
        measurement = metric.replace('/', '_')
        timestamp = datetime.strptime(date, '%Y-%m-%d')
        
        try:
            if metric == 'activities/heart':
                if 'activities-heart' in data and data['activities-heart']:
                    heart_data = data['activities-heart'][0]['value']
                    if 'restingHeartRate' in heart_data:
                        point = Point(measurement) \
                            .field('resting_heart_rate', heart_data['restingHeartRate']) \
                            .time(timestamp)
                        
                        # Add health condition tags
                        for condition, enabled in HEALTH_CONDITIONS.items():
                            if enabled:
                                point = point.tag(condition, 'true')
                        
                        points.append(point)
            
            elif metric == 'sleep':
                if 'sleep' in data and data['sleep']:
                    for sleep_record in data['sleep']:
                        if sleep_record.get('isMainSleep', True):  # Focus on main sleep
                            sleep_time = datetime.strptime(sleep_record['dateOfSleep'], '%Y-%m-%d')
                            
                            # Main sleep metrics
                            point = Point(measurement) \
                                .field('efficiency', sleep_record.get('efficiency', 0)) \
                                .field('minutes_asleep', sleep_record.get('minutesAsleep', 0)) \
                                .field('minutes_awake', sleep_record.get('minutesAwake', 0)) \
                                .field('time_in_bed', sleep_record.get('timeInBed', 0)) \
                                .time(sleep_time)
                            
                            # Add health condition tags
                            for condition, enabled in HEALTH_CONDITIONS.items():
                                if enabled:
                                    point = point.tag(condition, 'true')
                            
                            points.append(point)
            
            elif metric == 'hrv':
                if 'hrv' in data and data['hrv']:
                    for hrv_record in data['hrv']:
                        if 'value' in hrv_record and hrv_record['value']:
                            hrv_time = datetime.strptime(hrv_record['dateTime'], '%Y-%m-%d')
                            value = hrv_record['value']
                            
                            # Calculate LF/HF ratio for autonomic analysis
                            lf = value.get('lf', 0)
                            hf = value.get('hf', 1)  # Avoid division by zero
                            lf_hf_ratio = lf / hf if hf != 0 else 0
                            
                            point = Point(measurement) \
                                .field('daily_rmssd', value.get('dailyRmssd', 0)) \
                                .field('deep_rmssd', value.get('deepRmssd', 0)) \
                                .field('lf', lf) \
                                .field('hf', hf) \
                                .field('lf_hf_ratio', lf_hf_ratio) \
                                .time(hrv_time)
                            
                            # Add health condition tags
                            for condition, enabled in HEALTH_CONDITIONS.items():
                                if enabled:
                                    point = point.tag(condition, 'true')
                            
                            points.append(point)
            
            elif metric == 'spo2':
                if 'spo2' in data and data['spo2']:
                    for spo2_record in data['spo2']:
                        if 'value' in spo2_record:
                            spo2_time = datetime.strptime(spo2_record['dateTime'], '%Y-%m-%d')
                            point = Point(measurement) \
                                .field('value', spo2_record['value'].get('avg', 0)) \
                                .field('min', spo2_record['value'].get('min', 0)) \
                                .field('max', spo2_record['value'].get('max', 0)) \
                                .time(spo2_time)
                            points.append(point)
            
            else:
                # Generic handler for other metrics
                metric_key = metric.split('/')[-1]
                if metric_key in data and data[metric_key]:
                    for record in data[metric_key]:
                        if 'dateTime' in record and 'value' in record:
                            record_time = datetime.strptime(record['dateTime'], '%Y-%m-%d')
                            point = Point(measurement) \
                                .field('value', record['value']) \
                                .time(record_time)
                            points.append(point)
        
        except Exception as e:
            logger.error(f"Error processing {metric}: {str(e)}")
        
        return points
    
    def process_intraday_data(self, metric: str, date: str, detail_level: str = '1min') -> List[Point]:
        """Process intraday data with higher granularity"""
        endpoint = f"{metric}/intraday"
        data = self.fitbit_api.make_request(endpoint, date, detail_level)
        if not data:
            return []
        
        points = []
        measurement = f"{metric.replace('/', '_')}_intraday"
        
        try:
            # Find the intraday dataset
            intraday_key = None
            for key in data.keys():
                if 'intraday' in key.lower():
                    intraday_key = key
                    break
            
            if not intraday_key or 'dataset' not in data[intraday_key]:
                logger.warning(f"No intraday dataset found for {metric}")
                return []
            
            dataset = data[intraday_key]['dataset']
            logger.info(f"Processing {len(dataset)} intraday points for {metric}")
            
            for entry in dataset:
                if 'time' in entry and 'value' in entry:
                    # Parse time and create full timestamp
                    time_str = entry['time']
                    datetime_str = f"{date} {time_str}"
                    timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                    
                    point = Point(measurement) \
                        .field('value', entry['value']) \
                        .time(timestamp)
                    
                    # Add health condition tags for relevant metrics
                    if metric in ['activities/heart', 'hrv']:
                        for condition, enabled in HEALTH_CONDITIONS.items():
                            if enabled:
                                point = point.tag(condition, 'true')
                    
                    points.append(point)
        
        except Exception as e:
            logger.error(f"Error processing intraday {metric}: {str(e)}")
        
        return points
    
    def calculate_pem_risk(self, date: str) -> float:
        """Calculate PEM (Post-Exertional Malaise) risk score"""
        try:
            # This is a simplified PEM risk calculation
            # In practice, this would use historical data and more sophisticated algorithms
            risk_score = 0.0
            
            # Get today's heart rate data
            hr_data = self.fitbit_api.make_request('activities/heart', date)
            if hr_data and 'activities-heart' in hr_data and hr_data['activities-heart']:
                resting_hr = hr_data['activities-heart'][0]['value'].get('restingHeartRate', 0)
                if resting_hr > 100:  # Elevated resting heart rate
                    risk_score += 30
            
            # Get today's activity data
            steps_data = self.fitbit_api.make_request('activities/steps', date)  
            if steps_data and 'activities-steps' in steps_data and steps_data['activities-steps']:
                steps = steps_data['activities-steps'][0].get('value', 0)
                if steps > 5000:  # High activity for chronic illness
                    risk_score += 25
            
            # Get HRV data
            hrv_data = self.fitbit_api.make_request('hrv', date)
            if hrv_data and 'hrv' in hrv_data and hrv_data['hrv']:
                for hrv_record in hrv_data['hrv']:
                    if 'value' in hrv_record and hrv_record['value']:
                        daily_rmssd = hrv_record['value'].get('dailyRmssd', 0)
                        if daily_rmssd < 20:  # Low HRV indicates stress
                            risk_score += 45
                        break
            
            return min(risk_score, 100)  # Cap at 100
            
        except Exception as e:
            logger.error(f"Error calculating PEM risk: {str(e)}")
            return 0.0

def load_environment_from_file():
    """Load environment variables from .env file if it exists"""
    env_file = '.env'
    if os.path.exists(env_file):
        logger.info("Loading environment variables from .env file")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

def main():
    """Main execution function"""
    logger.info("Starting Enhanced Fitbit data sync...")
    
    # Load environment variables from .env file
    load_environment_from_file()
    
    # Validate configuration
    required_vars = ['FITBIT_CLIENT_ID', 'FITBIT_CLIENT_SECRET', 'FITBIT_ACCESS_TOKEN', 'FITBIT_REFRESH_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please check your .env file or environment variable configuration")
        return
    
    try:
        # Initialize components
        fitbit_api = FitbitAPI()
        influx_writer = InfluxDBWriter()
        processor = FitbitDataProcessor(fitbit_api, influx_writer)
        
        today = datetime.now().strftime('%Y-%m-%d')
        all_points = []
        
        # Collect daily summary data
        logger.info("Fetching daily summary data...")
        for metric in DAILY_METRICS:
            points = processor.process_daily_summary(metric, today)
            all_points.extend(points)
            time.sleep(0.5)  # Small delay to respect API limits
        
        # Collect intraday data
        logger.info("Fetching intraday data...")
        for metric in INTRADAY_METRICS:
            points = processor.process_intraday_data(metric, today)
            all_points.extend(points)
            time.sleep(0.5)  # Small delay to respect API limits
        
        # Calculate and add PEM risk score for Long COVID monitoring
        if HEALTH_CONDITIONS.get('long_covid') or HEALTH_CONDITIONS.get('pem'):
            logger.info("Calculating PEM risk score...")
            pem_risk = processor.calculate_pem_risk(today)
            
            pem_point = Point("pem_risk") \
                .field('risk_score', pem_risk) \
                .tag('long_covid', str(HEALTH_CONDITIONS.get('long_covid', False))) \
                .tag('pots', str(HEALTH_CONDITIONS.get('pots', False))) \
                .time(datetime.now())
            all_points.append(pem_point)
            
            logger.info(f"PEM Risk Score: {pem_risk}%")
        
        # Write all points to InfluxDB
        if all_points:
            influx_writer.write_points(all_points)
            logger.info(f"Successfully synced {len(all_points)} data points")
            
            # Print summary
            print(f"\n✅ Sync completed successfully!")
            print(f"📊 Data points collected: {len(all_points)}")
            print(f"📅 Date: {today}")
            print(f"🏥 Health conditions enabled: {[k for k, v in HEALTH_CONDITIONS.items() if v]}")
            
            if HEALTH_CONDITIONS.get('long_covid') or HEALTH_CONDITIONS.get('pem'):
                pem_risk = processor.calculate_pem_risk(today)
                print(f"⚠️  PEM Risk Score: {pem_risk}%")
                
                if pem_risk > 70:
                    print("🚨 HIGH PEM RISK - Consider resting today")
                elif pem_risk > 30:
                    print("⚡ MODERATE PEM RISK - Monitor activity levels")
                else:
                    print("✅ LOW PEM RISK - Normal activity tolerance")
        else:
            logger.warning("No data points collected")
            print("⚠️  No data collected. Check your Fitbit device sync and API credentials.")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        print(f"❌ Error: {str(e)}")
        print("Check the fitbit_sync.log file for detailed error information.")
    
    finally:
        try:
            influx_writer.close()
        except:
            pass
        logger.info("Fitbit data sync completed")

if __name__ == "__main__":
    main()