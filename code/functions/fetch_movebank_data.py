"""
Movebank Data Fetcher for Study ID: 3445611111
This script fetches GPS, acceleration, and other sensor data from Movebank
Requires: requests, pandas
Set environment variables: mbus (username) and mbpw (password)
"""

import requests
import os
import hashlib
import csv
import json
import io
import pandas as pd
import argparse
from datetime import datetime
from pathlib import Path

class MovebankDataFetcher:
    def __init__(self, username=None, password=None):
        """Initialize with Movebank credentials from environment, config file, or parameters"""
        # Try to get credentials from: 1) parameters, 2) environment variables, 3) config file
        self.username = username or os.environ.get('mbus')
        self.password = password or os.environ.get('mbpw')

        # If still not set, try to load from config.py
        if not self.username or not self.password:
            try:
                import config
                self.username = self.username or config.MOVEBANK_USERNAME
                self.password = self.password or config.MOVEBANK_PASSWORD
            except (ImportError, AttributeError):
                pass

        self.base_url = 'https://www.movebank.org/movebank/service/direct-read'

        if not self.username or not self.password:
            raise ValueError(
                "Credentials not found. Please either:\n"
                "1. Edit config.py with your Movebank username and password, OR\n"
                "2. Set environment variables: mbus and mbpw, OR\n"
                "3. Pass credentials directly to MovebankDataFetcher(username, password)"
            )

        # Don't allow default placeholder credentials
        if self.username == 'your_username' or self.password == 'your_password':
            raise ValueError(
                "Please edit config.py and replace 'your_username' and 'your_password' "
                "with your actual Movebank credentials"
            )

    def call_api(self, params):
        """Make API request with automatic license acceptance"""
        response = requests.get(
            self.base_url,
            params=params,
            auth=(self.username, self.password)
        )

        print(f"Request: {response.url}")

        if response.status_code == 200:
            # Check if license terms need to be accepted
            if 'License Terms:' in str(response.content):
                print("License terms detected - accepting automatically...")
                hash_val = hashlib.md5(response.content).hexdigest()
                params_list = list(params) if isinstance(params, tuple) else list(params.items())
                params_list.append(('license-md5', hash_val))

                response = requests.get(
                    self.base_url,
                    params=params_list,
                    cookies=response.cookies,
                    auth=(self.username, self.password)
                )

                if response.status_code == 403:
                    print("Error: Incorrect license hash")
                    return None

            return response.content.decode('utf-8')

        elif response.status_code == 403:
            print(f"Error 403: Access denied. Check your permissions for this study.")
        else:
            print(f"Error {response.status_code}: {response.content.decode('utf-8')}")

        return None

    def get_study_info(self, study_id):
        """Get study metadata"""
        params = {'entity_type': 'study', 'study_id': study_id}
        result = self.call_api(params)

        if result:
            df = pd.read_csv(io.StringIO(result))
            return df
        return None

    def get_sensor_types(self, study_id):
        """Get available sensor types in the study"""
        params = {'entity_type': 'sensor', 'tag_study_id': study_id}
        result = self.call_api(params)

        if result:
            df = pd.read_csv(io.StringIO(result))
            return df
        return None

    def get_individuals(self, study_id):
        """Get all individuals/animals in the study"""
        params = {'entity_type': 'individual', 'study_id': study_id}
        result = self.call_api(params)

        if result:
            df = pd.read_csv(io.StringIO(result))
            return df
        return None

    def get_tags(self, study_id):
        """Get all tags in the study"""
        params = {'entity_type': 'tag', 'study_id': study_id}
        result = self.call_api(params)

        if result:
            df = pd.read_csv(io.StringIO(result))
            return df
        return None

    def get_deployments(self, study_id):
        """Get all deployments in the study"""
        params = {'entity_type': 'deployment', 'study_id': study_id}
        result = self.call_api(params)

        if result:
            df = pd.read_csv(io.StringIO(result))
            return df
        return None

    def get_study_attributes(self, study_id, sensor_type_id=None):
        """Get available attributes for a sensor type"""
        params = {'entity_type': 'study_attribute', 'study_id': study_id}
        if sensor_type_id:
            params['sensor_type_id'] = sensor_type_id

        result = self.call_api(params)

        if result:
            df = pd.read_csv(io.StringIO(result))
            return df
        return None

    def get_event_data(self, study_id, sensor_type_id=None, individual_id=None,
                      attributes='all', timestamp_start=None, timestamp_end=None):
        """
        Get event data (tracking/sensor data)

        Parameters:
        - study_id: Study ID
        - sensor_type_id: 653 for GPS, 2365683 for Acceleration, etc.
        - individual_id: Specific individual ID (optional)
        - attributes: 'all' or comma-separated list of attributes
        - timestamp_start: Start timestamp in format 'yyyyMMddHHmmssSSS'
        - timestamp_end: End timestamp in format 'yyyyMMddHHmmssSSS'
        """
        params = {
            'entity_type': 'event',
            'study_id': study_id,
            'attributes': attributes
        }

        if sensor_type_id:
            params['sensor_type_id'] = sensor_type_id
        if individual_id:
            params['individual_id'] = individual_id
        if timestamp_start:
            params['timestamp_start'] = timestamp_start
        if timestamp_end:
            params['timestamp_end'] = timestamp_end

        result = self.call_api(params)

        if result:
            df = pd.read_csv(io.StringIO(result))
            return df
        return None

    def fetch_all_study_data(self, study_id, output_dir='movebank_data',
                            sensor_types=None, timestamp_start=None, timestamp_end=None,
                            fetch_metadata=True):
        """
        Fetch all available data from a study and save to files

        Parameters:
        - study_id: Movebank study ID
        - output_dir: Directory to save output files
        - sensor_types: List of sensor type IDs to fetch (None = all sensors)
        - timestamp_start: Start timestamp in format 'YYYY-MM-DD' or 'yyyyMMddHHmmssSSS'
        - timestamp_end: End timestamp in format 'YYYY-MM-DD' or 'yyyyMMddHHmmssSSS'
        - fetch_metadata: Whether to fetch metadata (individuals, tags, deployments)
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        # Convert timestamps if provided in simple format
        ts_start = self._convert_timestamp(timestamp_start) if timestamp_start else None
        ts_end = self._convert_timestamp(timestamp_end) if timestamp_end else None

        print(f"\n{'='*60}")
        print(f"Fetching data for Study ID: {study_id}")
        if ts_start or ts_end:
            print(f"Time range: {timestamp_start or 'beginning'} to {timestamp_end or 'now'}")
        if sensor_types:
            print(f"Sensors: {sensor_types}")
        print(f"{'='*60}\n")

        # 1. Get study info
        print("1. Fetching study information...")
        study_info = self.get_study_info(study_id)
        if study_info is not None:
            study_info.to_csv(output_path / 'study_info.csv', index=False)
            print(f"   ✓ Study: {study_info['name'].iloc[0] if 'name' in study_info.columns else 'N/A'}")
            print(f"   ✓ Saved to: study_info.csv")
        else:
            print("   ✗ Failed to fetch study info")
            return

        if fetch_metadata:
            # 2. Get individuals
            print("\n2. Fetching individuals...")
            individuals = self.get_individuals(study_id)
            if individuals is not None:
                individuals.to_csv(output_path / 'individuals.csv', index=False)
                print(f"   ✓ Found {len(individuals)} individuals")
                print(f"   ✓ Saved to: individuals.csv")

            # 3. Get tags
            print("\n3. Fetching tags...")
            tags = self.get_tags(study_id)
            if tags is not None:
                tags.to_csv(output_path / 'tags.csv', index=False)
                print(f"   ✓ Found {len(tags)} tags")
                print(f"   ✓ Saved to: tags.csv")

            # 4. Get deployments
            print("\n4. Fetching deployments...")
            deployments = self.get_deployments(study_id)
            if deployments is not None:
                deployments.to_csv(output_path / 'deployments.csv', index=False)
                print(f"   ✓ Found {len(deployments)} deployments")
                print(f"   ✓ Saved to: deployments.csv")

        # 5. Get sensor types
        print(f"\n{5 if fetch_metadata else 2}. Fetching sensor types...")
        sensors = self.get_sensor_types(study_id)
        if sensors is not None:
            sensors.to_csv(output_path / 'sensors.csv', index=False)
            print(f"   ✓ Found {len(sensors)} sensor types")
            print(f"   ✓ Saved to: sensors.csv")

            # Get unique sensor type IDs
            available_sensor_ids = sensors['sensor_type_id'].unique()

            # Filter by requested sensor types if specified
            if sensor_types:
                sensor_type_ids = [sid for sid in sensor_types if sid in available_sensor_ids]
                if len(sensor_type_ids) != len(sensor_types):
                    missing = set(sensor_types) - set(available_sensor_ids)
                    print(f"   ⚠ Warning: Requested sensor types not found in study: {missing}")
            else:
                sensor_type_ids = available_sensor_ids

            # Sensor type mapping
            sensor_names = {
                653: 'gps',
                2365683: 'acceleration',
                397: 'bird_ring',
                673: 'radio_transmitter',
                82798: 'argos',
                2365682: 'natural_mark',
                3886361: 'solar_geolocator',
                7842954: 'accessory_measurements',
                77740391: 'barometer',
                77740402: 'magnetometer',
                819073350: 'orientation',
                1297673380: 'gyroscope',
                2206221896: 'heart_rate'
            }

            # 6. Get event data for each sensor type
            print(f"\n{6 if fetch_metadata else 3}. Fetching event data by sensor type...")
            for sensor_id in sensor_type_ids:
                sensor_name = sensor_names.get(sensor_id, f'sensor_{sensor_id}')
                print(f"\n   Fetching {sensor_name} data (sensor_type_id={sensor_id})...")

                try:
                    events = self.get_event_data(
                        study_id,
                        sensor_type_id=sensor_id,
                        timestamp_start=ts_start,
                        timestamp_end=ts_end
                    )
                    if events is not None and len(events) > 0:
                        filename = f'events_{sensor_name}.csv'
                        events.to_csv(output_path / filename, index=False)
                        print(f"   ✓ Found {len(events)} events")
                        print(f"   ✓ Saved to: {filename}")
                    else:
                        print(f"   - No events found for {sensor_name}")
                except Exception as e:
                    print(f"   ✗ Error fetching {sensor_name}: {str(e)}")

        print(f"\n{'='*60}")
        print(f"Data export complete! Files saved to: {output_path.absolute()}")
        print(f"{'='*60}\n")

    def _convert_timestamp(self, timestamp_str):
        """
        Convert timestamp from various formats to Movebank format (yyyyMMddHHmmssSSS)
        Accepts: 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS', or already formatted string
        """
        if not timestamp_str:
            return None

        # If already in Movebank format (17 digits), return as-is
        if timestamp_str.replace(' ', '').replace('-', '').replace(':', '').isdigit() and len(timestamp_str.replace(' ', '').replace('-', '').replace(':', '')) >= 14:
            return timestamp_str.replace(' ', '').replace('-', '').replace(':', '').ljust(17, '0')

        # Try to parse common date formats
        try:
            # Try YYYY-MM-DD
            if len(timestamp_str) == 10 and timestamp_str.count('-') == 2:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d')
                return dt.strftime('%Y%m%d%H%M%S000')

            # Try YYYY-MM-DD HH:MM:SS
            elif len(timestamp_str) == 19:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                return dt.strftime('%Y%m%d%H%M%S000')

            # Try YYYY-MM-DD HH:MM
            elif len(timestamp_str) == 16:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M')
                return dt.strftime('%Y%m%d%H%M%S000')

        except ValueError:
            pass

        return timestamp_str


def main():
    """Main function to fetch data from study with command-line arguments"""

    # Sensor type mapping for easy reference
    SENSOR_TYPES = {
        'gps': 653,
        'acceleration': 2365683,
        'acc': 2365683,
        'bird_ring': 397,
        'radio_transmitter': 673,
        'radio': 673,
        'argos': 82798,
        'natural_mark': 2365682,
        'solar_geolocator': 3886361,
        'geolocator': 3886361,
        'accessory_measurements': 7842954,
        'accessory': 7842954,
        'barometer': 77740391,
        'magnetometer': 77740402,
        'orientation': 819073350,
        'gyroscope': 1297673380,
        'heart_rate': 2206221896
    }

    parser = argparse.ArgumentParser(
        description='Fetch data from Movebank study 3445611111',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Fetch all data (default)
  python fetch_movebank_data.py

  # Fetch only GPS data
  python fetch_movebank_data.py --sensors gps

  # Fetch GPS and acceleration data
  python fetch_movebank_data.py --sensors gps acceleration

  # Fetch data from specific time period
  python fetch_movebank_data.py --start 2024-01-01 --end 2024-12-31

  # Fetch GPS data from January 2024
  python fetch_movebank_data.py --sensors gps --start 2024-01-01 --end 2024-01-31

  # Skip metadata, only fetch GPS events
  python fetch_movebank_data.py --sensors gps --no-metadata

  # Use sensor type IDs directly
  python fetch_movebank_data.py --sensors 653 2365683

Sensor Types:
  gps (653), acceleration/acc (2365683), bird_ring (397),
  radio_transmitter/radio (673), argos (82798), natural_mark (2365682),
  solar_geolocator/geolocator (3886361), accessory_measurements/accessory (7842954),
  barometer (77740391), magnetometer (77740402), orientation (819073350),
  gyroscope (1297673380), heart_rate (2206221896)
        '''
    )

    parser.add_argument(
        '--study-id',
        type=int,
        default=3445611111,
        help='Movebank study ID (default: 3445611111)'
    )

    parser.add_argument(
        '--sensors',
        nargs='+',
        help='Sensor types to fetch (names or IDs). Leave empty to fetch all sensors.'
    )

    parser.add_argument(
        '--start',
        help='Start date/time (YYYY-MM-DD or "YYYY-MM-DD HH:MM:SS")'
    )

    parser.add_argument(
        '--end',
        help='End date/time (YYYY-MM-DD or "YYYY-MM-DD HH:MM:SS")'
    )

    parser.add_argument(
        '--output',
        default='movebank_data',
        help='Output directory (default: movebank_data)'
    )

    parser.add_argument(
        '--no-metadata',
        action='store_true',
        help='Skip fetching metadata (individuals, tags, deployments)'
    )

    parser.add_argument(
        '--list-sensors',
        action='store_true',
        help='List available sensors in the study and exit'
    )

    args = parser.parse_args()

    try:
        # Initialize fetcher
        fetcher = MovebankDataFetcher()

        # List sensors and exit if requested
        if args.list_sensors:
            print(f"\nFetching available sensors for study {args.study_id}...\n")
            sensors = fetcher.get_sensor_types(args.study_id)
            if sensors is not None and len(sensors) > 0:
                print("Available sensor types:")
                print("-" * 60)
                sensor_names = {
                    653: 'GPS',
                    2365683: 'Acceleration',
                    397: 'Bird Ring',
                    673: 'Radio Transmitter',
                    82798: 'Argos Doppler Shift',
                    2365682: 'Natural Mark',
                    3886361: 'Solar Geolocator',
                    7842954: 'Accessory Measurements',
                    77740391: 'Barometer',
                    77740402: 'Magnetometer',
                    819073350: 'Orientation',
                    1297673380: 'Gyroscope',
                    2206221896: 'Heart Rate'
                }
                for _, row in sensors.iterrows():
                    sensor_id = row['sensor_type_id']
                    sensor_name = sensor_names.get(sensor_id, 'Unknown')
                    print(f"  {sensor_name:30} (ID: {sensor_id})")
                print("-" * 60)
            else:
                print("No sensors found or failed to fetch sensor information.")
            return

        # Parse sensor types
        sensor_type_ids = None
        if args.sensors:
            sensor_type_ids = []
            for sensor in args.sensors:
                # Check if it's a name or ID
                if sensor.lower() in SENSOR_TYPES:
                    sensor_type_ids.append(SENSOR_TYPES[sensor.lower()])
                elif sensor.isdigit():
                    sensor_type_ids.append(int(sensor))
                else:
                    print(f"Warning: Unknown sensor type '{sensor}' - skipping")

        # Fetch data
        fetcher.fetch_all_study_data(
            study_id=args.study_id,
            output_dir=args.output,
            sensor_types=sensor_type_ids,
            timestamp_start=args.start,
            timestamp_end=args.end,
            fetch_metadata=not args.no_metadata
        )

    except ValueError as e:
        print(f"Error: {e}")
        print("\nPlease set your Movebank credentials:")
        print("  Windows (CMD): set mbus=your_username && set mbpw=your_password")
        print("  Windows (PowerShell): $env:mbus='your_username'; $env:mbpw='your_password'")
        print("  Linux/Mac: export mbus=your_username && export mbpw=your_password")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()