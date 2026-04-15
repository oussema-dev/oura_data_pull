"""
Oura API v2 daily data puller.

1. Pulls daily Oura data for every rider from January 1, 2025 until today on first run.
2. On subsequent runs, repulls the last synced day per rider and then pulls forward until today.
3. Combines all requested routes into one row per rider per date.
4. Saves to a CSV dataframe.
5. Logs everything to log_oura.txt.
6. Uses BlockingScheduler to run every day at 4:00 AM after the first immediate run.
7. Optionally emails coaches if a rider appears unsynced for the past 3 days.
8. Logs the number of API requests made every minute in a separate monitor.

- This script is intentionally modular and defensive.
- Empty or malformed responses should not stop the pipeline.
"""

import os
import time
import logging
import requests
import smtplib
import threading
import pandas as pd
from datetime import datetime, timedelta, date
from dateutil.parser import isoparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from apscheduler.schedulers.blocking import BlockingScheduler
from config import RIDER_LIST, DEFAULT_START_DATE, CSV_FILE_FULL, LOG_FILE, DEFAULT_TZ_OFFSET, EMAIL_SERVER, EMAIL_PORT, EMAIL_SENDER, EMAIL_PASSWORD, CC_EMAIL

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Global API Request Counter and Lock
api_calls_count = 0
api_counter_lock = threading.Lock()

# Global Timezone Cache to hold the last known timezone per rider
tz_cache = {}

# API REQUEST COUNTER FUNCTIONALITY
def log_api_requests_per_minute():
    """Logs and prints the number of API requests made in the last minute."""
    global api_calls_count
    with api_counter_lock:
        count = api_calls_count
        api_calls_count = 0
    
    if count > 0:
        logging.info(f"API Requests in the last minute: {count}")

# EMAIL NOTIFICATION FUNCTIONALITY
def send_inactivity_email(rider_name, coach_email, last_synced_date):
    """Sends an email to the coach if the rider hasn't synced in 3+ days."""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = coach_email
        msg['Cc'] = CC_EMAIL
        msg['Subject'] = f"Action Required: Oura Ring Sync Reminder for {rider_name}"
        
        body = (f"Hello,\n\n"
                f"This is an automated alert that {rider_name} has not synced their Oura ring "
                f"for the past 3 or more days.\n"
                f"The last successfully synced date is: {last_synced_date}.\n\n"
                f"Please remind them to open the Oura app and sync their ring.\n\n"
                f"Best,\nOura Data Automator")
        
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        
        recipients = [coach_email, CC_EMAIL]
        server.sendmail(EMAIL_SENDER, recipients, msg.as_string())
        server.quit()
        logging.info(f"Inactivity email sent to {coach_email} for rider {rider_name}")
    except Exception as e:
        logging.error(f"Failed to send email for {rider_name}: {e}")

def check_inactivity(df):
    """Checks the dataframe for riders with no data in the past 3 days and sends emails."""
    today = date.today()
    for rider_name, info in RIDER_LIST.items():
        rider_df = df[df['rider_name'] == rider_name]
        if not rider_df.empty:
            last_date_str = rider_df['date'].max()
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
        else:
            last_date = DEFAULT_START_DATE
            
        days_missed = (today - last_date).days
        if days_missed >= 3:
            send_inactivity_email(rider_name, info['coach_email'], last_date)

# API FETCHING & DATA EXTRACTION
def make_api_call(url, token, params):
    """Generic function to make API calls with error handling and rate limit backoff."""
    global api_calls_count
    headers = {"Authorization": f"Bearer {token}"}
    
    retries = 3
    for attempt in range(retries):
        with api_counter_lock:
            api_calls_count += 1
            
        try:
            response = requests.get(url, headers=headers, params=params)
            status = response.status_code
            
            if status == 200:
                return response.json()
            elif status == 429:
                logging.warning(f"Rate limit hit. Retrying in 5 seconds (Attempt {attempt+1}/{retries})")
                time.sleep(5)
            elif status in [400, 401, 403, 404, 422]:
                logging.error(f"API Error {status} on route {url}: {response.text}")
                return {"data": []} # Return empty to prevent crash
            else:
                logging.error(f"Unexpected status {status} on route {url}: {response.text}")
                return {"data": []}
                
        except Exception as e:
            logging.error(f"Request exception on {url}: {e}")
            return {"data": []}
            
    return {"data": []}

def get_tz_offset(token, rider_name, target_date):
    """Retrieves timezone offset by looking at sleep data up to 7 days back."""
    # Check cache first (set during sleep processing)
    if rider_name in tz_cache:
        return tz_cache[rider_name]
    
    # Look back up to 7 days
    url = 'https://api.ouraring.com/v2/usercollection/sleep'
    for i in range(7):
        query_date = target_date - timedelta(days=i)
        params = {"start_date": str(query_date), "end_date": str(query_date + timedelta(days=1))}
        data = make_api_call(url, token, params)
        for item in data.get("data", []):
            if item.get("type") == "long_sleep" and item.get("bedtime_start"):
                dt = isoparse(item["bedtime_start"])
                # Extract TZ string like "+01:00" or "-05:00" or "Z"
                tz_str = dt.strftime('%z')
                if tz_str:
                    tz_formatted = tz_str[:3] + ':' + tz_str[3:]
                    tz_cache[rider_name] = tz_formatted
                    return tz_formatted
    
    # If not found at all
    return DEFAULT_TZ_OFFSET

# Route Specific Data Processors
def fetch_activity(token, start_str, end_str):
    """Fetch daily activity data for the requested date."""
    url = 'https://api.ouraring.com/v2/usercollection/daily_activity'
    res = make_api_call(url, token, {"start_date": start_str, "end_date": end_str})
    for item in res.get("data", []):
        if item.get("day") == start_str:
            return {
                "daily_activity_non_wear_time": item.get("non_wear_time"),
                "daily_activity_high_activity_time": item.get("high_activity_time")
            }
    return {"daily_activity_non_wear_time": None, "daily_activity_high_activity_time": None}

def fetch_readiness(token, start_str, end_str):
    """Fetch daily readiness data for the requested date.

    If multiple items exist, only the one whose day equals start_date is used.
    """
    url = 'https://api.ouraring.com/v2/usercollection/daily_readiness'
    res = make_api_call(url, token, {"start_date": start_str, "end_date": end_str})
    for item in res.get("data", []):
        if item.get("day") == start_str:
            return {
                "daily_readiness_temperature_deviation": item.get("temperature_deviation"),
                "daily_readiness_temperature_trend_deviation": item.get("temperature_trend_deviation")
            }
    return {"daily_readiness_temperature_deviation": None, "daily_readiness_temperature_trend_deviation": None}

def fetch_spo2(token, start_str, end_str):
    """Fetch daily SpO2 data for the requested date.

    Only the average value inside spo2_percentage is kept, and only if the
    nested structure is a populated dictionary.
    """
    url = 'https://api.ouraring.com/v2/usercollection/daily_spo2'
    res = make_api_call(url, token, {"start_date": start_str, "end_date": end_str})
    for item in res.get("data", []):
        if item.get("day") == start_str:
            spo2_dict = item.get("spo2_percentage")
            avg = spo2_dict.get("average") if isinstance(spo2_dict, dict) else None
            return {"daily_spo2_percentage": avg}
    return {"daily_spo2_percentage": None}

def fetch_stress(token, start_str, end_str):
    """Fech daily stress data for the requested date."""
    url = 'https://api.ouraring.com/v2/usercollection/daily_stress'
    res = make_api_call(url, token, {"start_date": start_str, "end_date": end_str})
    for item in res.get("data", []):
        if item.get("day") == start_str:
            return {
                "daily_stress_stress_high": item.get("stress_high"),
                "daily_stress_recovery_high": item.get("recovery_high")
            }
    return {"daily_stress_stress_high": None, "daily_stress_recovery_high": None}

def fetch_sleep(token, start_str, end_str, rider_name):
    """Fetch sleep data."""
    url = 'https://api.ouraring.com/v2/usercollection/sleep'
    res = make_api_call(url, token, {"start_date": start_str, "end_date": end_str})
    
    cols = ["average_breath", "average_heart_rate", "average_hrv", "awake_time", 
            "bedtime_start", "bedtime_end", "deep_sleep_duration", "efficiency", 
            "latency", "lowest_heart_rate", "restless_periods", "time_in_bed", "total_sleep_duration"]
    
    out = {f"sleep_{c}": None for c in cols}
    out.update({
        "sleep_hr_min": None, "sleep_hr_max": None, "sleep_hr_avg": None, "sleep_min_sleep_hr_timestamp": None,
        "sleep_hrv_min": None, "sleep_hrv_max": None, "sleep_hrv_avg": None
    })

    for item in res.get("data", []):
        if item.get("type") == "long_sleep":
            # Extract basic cols
            for c in cols:
                out[f"sleep_{c}"] = item.get(c)
                
            # Cache timezone
            bt_start = item.get("bedtime_start")
            if bt_start:
                dt = isoparse(bt_start)
                tz_str = dt.strftime('%z')
                if tz_str:
                    tz_formatted = tz_str[:3] + ':' + tz_str[3:]
                    tz_cache[rider_name] = tz_formatted
            
            # Process Heart Rate
            hr_data = item.get("heart_rate")
            if isinstance(hr_data, dict) and hr_data.get("items"):
                interval = hr_data.get("interval", 300.0)
                ts_start = hr_data.get("timestamp")
                valid_hr = []
                for idx, val in enumerate(hr_data.get("items")):
                    if val is not None and 30 <= val <= 240:
                        valid_hr.append((idx, val))
                
                if valid_hr:
                    out["sleep_hr_min"] = min(v[1] for v in valid_hr)
                    out["sleep_hr_max"] = max(v[1] for v in valid_hr)
                    out["sleep_hr_avg"] = sum(v[1] for v in valid_hr) / len(valid_hr)
                    
                    # Find min HR timestamp
                    min_idx = min(valid_hr, key=lambda x: x[1])[0]
                    if ts_start:
                        min_dt = isoparse(ts_start) + timedelta(seconds=min_idx * interval)
                        out["sleep_min_sleep_hr_timestamp"] = min_dt.strftime('%H:%M:%S')

            # Process HRV
            hrv_data = item.get("hrv")
            if isinstance(hrv_data, dict) and hrv_data.get("items"):
                valid_hrv = [val for val in hrv_data.get("items") if val is not None and 10 <= val <= 200]
                if valid_hrv:
                    out["sleep_hrv_min"] = min(valid_hrv)
                    out["sleep_hrv_max"] = max(valid_hrv)
                    out["sleep_hrv_avg"] = sum(valid_hrv) / len(valid_hrv)
                    
            return out # Return on first long_sleep found

    return out

def fetch_heartrate(token, start_date_obj, rider_name):
    """Parse heartrate data."""
    url = 'https://api.ouraring.com/v2/usercollection/heartrate'
    
    # Determine timezone offset
    tz_offset = get_tz_offset(token, rider_name, start_date_obj)
    
    # Target window: 7 PM of start_date to 11 AM of start_date + 1 (which is end_date)
    start_dt_str = f"{start_date_obj}T19:00:00{tz_offset}"
    end_dt_str = f"{(start_date_obj + timedelta(days=1))}T11:00:00{tz_offset}"
    
    params = {"start_datetime": start_dt_str, "end_datetime": end_dt_str}
    res = make_api_call(url, token, params)
    
    out = {
        "heartrate_min": None, "heartrate_max": None, "heartrate_avg": None,
        "heartrate_min_bpm_timestamp": None,
        "heartrate_bpm_values": None, "heartrate_bpm_timestamps": None
    }
    
    # Parse Offset into timedelta to adjust UTC API responses
    sign = -1 if tz_offset[0] == '-' else 1
    hrs, mins = map(int, tz_offset[1:].split(':'))
    offset_td = timedelta(hours=sign * hrs, minutes=sign * mins)
    
    valid_bpms = []
    
    for item in res.get("data", []):
        bpm = item.get("bpm")
        ts_utc_str = item.get("timestamp")
        
        if bpm is not None and 30 <= bpm <= 240 and ts_utc_str:
            # Check if within window (Oura API sometimes returns padding)
            dt_utc = isoparse(ts_utc_str)
            dt_local = dt_utc + offset_td
            
            # Reconstruct bound objects for comparison
            bound_start = isoparse(start_dt_str)
            bound_end = isoparse(end_dt_str)
            
            # Remove tzinfo for naive comparison (we already adjusted the raw time)
            dt_local_naive = dt_local.replace(tzinfo=None)
            bound_start_naive = bound_start.replace(tzinfo=None)
            bound_end_naive = bound_end.replace(tzinfo=None)
            
            if bound_start_naive <= dt_local_naive <= bound_end_naive:
                valid_bpms.append((bpm, dt_local_naive.strftime('%H:%M:%S')))
                
    if valid_bpms:
        bpm_vals = [str(v[0]) for v in valid_bpms]
        ts_vals = [v[1] for v in valid_bpms]
        
        min_tuple = min(valid_bpms, key=lambda x: x[0])
        
        out["heartrate_min"] = min_tuple[0]
        out["heartrate_max"] = max(int(b) for b in bpm_vals)
        out["heartrate_avg"] = sum(int(b) for b in bpm_vals) / len(bpm_vals)
        out["heartrate_min_bpm_timestamp"] = min_tuple[1]
        out["heartrate_bpm_values"] = ",".join(bpm_vals)
        out["heartrate_bpm_timestamps"] = ",".join(ts_vals)
        
    return out

# MAIN DATA PIPELINE
def run_sync():
    """Main function that orchestrates pulling data, transforming, and saving."""
    logging.info("Starting Oura Data Sync...")
    
    # Load existing data to figure out max date per rider
    if os.path.exists(CSV_FILE_FULL):
        df_existing = pd.read_csv(CSV_FILE_FULL)
    else:
        df_existing = pd.DataFrame()
        
    today = date.today()
    new_rows_count = 0
    all_new_rows = []

    for rider_name, info in RIDER_LIST.items():
        # Determine start date
        start_date = DEFAULT_START_DATE
        if not df_existing.empty and rider_name in df_existing['rider_name'].values:
            # Get max date and repull it
            max_date_str = df_existing[df_existing['rider_name'] == rider_name]['date'].max()
            start_date = datetime.strptime(max_date_str, "%Y-%m-%d").date()

        current_date = start_date
        while current_date <= today:
            logging.info(f"Pulling data for Rider: {rider_name} | Date: {current_date}")
            
            str_date = str(current_date)
            str_next_date = str(current_date + timedelta(days=1))
            token = info["token"]
            
            # Compile row dictionary
            row = {
                "date": str_date,
                "rider_name": rider_name,
                "rider_email": info["email"],
                "coach": info["coach"],
                "coach_email": info["coach_email"]
            }
            
            # Fetch Routes
            act_data = fetch_activity(token, str_date, str_next_date)
            read_data = fetch_readiness(token, str_date, str_next_date)
            spo2_data = fetch_spo2(token, str_date, str_next_date)
            stress_data = fetch_stress(token, str_date, str_next_date)
            sleep_data = fetch_sleep(token, str_date, str_next_date, rider_name)
            hr_data = fetch_heartrate(token, current_date, rider_name)
            
            row.update(act_data)
            row.update(read_data)
            row.update(spo2_data)
            row.update(stress_data)
            row.update(sleep_data)
            row.update(hr_data)
            
            all_new_rows.append(row)
            new_rows_count += 1
            current_date += timedelta(days=1)
            
    if all_new_rows:
        df_new = pd.DataFrame(all_new_rows)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        
        # Data Cleaning
        # Drop duplicates (keep last for repulled latest day)
        base_cols = ["date", "rider_name", "rider_email", "coach", "coach_email"]
        df_combined.drop_duplicates(subset=base_cols, keep='last', inplace=True)
        
        # Drop rows where all data columns are None/Empty
        data_cols = [c for c in df_combined.columns if c not in base_cols]
        df_combined.dropna(subset=data_cols, how='all', inplace=True)
        
        # Sort by date
        df_combined.sort_values(by="date", inplace=True)
        
        # Check Inactivity based on complete records BEFORE removing today
        check_inactivity(df_combined)

        # Remove 'today' row to avoid partial data
        str_today = str(today)
        df_combined = df_combined[df_combined['date'] != str_today]
        
        # Save Full CSV
        df_combined.to_csv(CSV_FILE_FULL, index=False)
        logging.info(f"Full CSV updated. New valid rows processed: {new_rows_count}")
    else:
        logging.info("No new data to process.")

# SCHEDULER & ENTRY POINT
if __name__ == "__main__":
    logging.info("Initializing Oura API Pipeline...")
    
    # Run API Request logger in background thread (every 60s)
    def minute_logger():
        while True:
            time.sleep(60)
            log_api_requests_per_minute()
    threading.Thread(target=minute_logger, daemon=True).start()
    
    
    # Execute immediately on start
    run_sync()
    
    # Schedule to run every day at 04:00 AM
    scheduler = BlockingScheduler()
    scheduler.add_job(run_sync, 'cron', hour=4, minute=0)
    
    logging.info("Scheduler started. Pipeline will run daily at 4:00 AM.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped.")