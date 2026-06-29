import xml.etree.ElementTree as ET
import mysql.connector
import os
from lxml import etree

NS = {'sdms': 'http://sdms/stream'}

class StreamConfig:
    def __init__(self, name, window_type, window_size=None, window_unit=None):
        self.name = name
        self.window_type = window_type
        self.window_size = window_size
        self.window_unit = window_unit

class InputMonitor:
    def __init__(self, db_config):
        self.db_config = db_config
        self.streams = {}
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Successfully connected to the database.")
        except Exception as e:
            print(f"Error connecting to database. Make sure MySQL is running. {e}")
            print("Running in simulation mode (printing queries).")

    def update_tick(self, tick_value):
        if not self.conn:
            return
        # Set system tick to the logical timestamp of the data we just processed
        query = "UPDATE system_tick SET current_tick = %s WHERE id = 1"
        self.cursor.execute(query, (tick_value,))
        self.conn.commit()

    def validate_xml(self, xml_path, xsd_path):
        
        try:
            xmlschema_doc = etree.parse(xsd_path)
            xmlschema = etree.XMLSchema(xmlschema_doc)
            xml_doc = etree.parse(xml_path)
            if xmlschema.validate(xml_doc):
                return True
            else:
                print(f"XML Validation Failed for {xml_path}")
                for error in xmlschema.error_log:
                    print(f"      - {error.message}")
                return False
        except Exception as e:
            print(f"Error during validation: {e}")
            return False

    def parse_traffic_config(self, config_path):
        print(f"[InputMonitor] Loading stream configuration from {config_path}...")
        try:
            tree = etree.parse(config_path)
            root = tree.getroot()

            
            for stream_elem in root.findall('sdms:stream', NS):
                name_elem = stream_elem.find('sdms:name', NS)
                if name_elem is None:
                    name_elem = stream_elem.find('name')
                
                if name_elem is None or not name_elem.text:
                    print(f"[InputMonitor] WARNING: Stream found but no name, skipping")
                    continue
                
                name = name_elem.text
                
                window_elem = stream_elem.find('sdms:window', NS)
                if window_elem is None:
                    window_elem = stream_elem.find('window')
                
                window_type = None
                window_size = None
                window_unit = None
                
                if window_elem is not None:
                    wt_elem = window_elem.find('sdms:windowType', NS)
                    if wt_elem is None:
                        wt_elem = window_elem.find('windowType')
                    
                    ws_elem = window_elem.find('sdms:size', NS)
                    if ws_elem is None:
                        ws_elem = window_elem.find('size')
                    
                    wu_elem = window_elem.find('sdms:unit', NS)
                    if wu_elem is None:
                        wu_elem = window_elem.find('unit')
                    
                    window_type = wt_elem.text if wt_elem is not None else None
                    window_size = int(ws_elem.text) if ws_elem is not None else None
                    window_unit = wu_elem.text if wu_elem is not None else None
                
                # create stream name with suffix for database table
                stream_name = f"{name}_stream"
                self.streams[stream_name] = StreamConfig(stream_name, window_type, window_size, window_unit)
                print(f"[InputMonitor] Registered Stream: {stream_name} | Window: {window_type} ({window_size} {window_unit})")
        
        except Exception as e:
            print(f"[InputMonitor] Error parsing traffic config: {e}")
            import traceback
            traceback.print_exc()

    def maintain_window(self, stream_name):
        
        config = self.streams.get(stream_name)
        if not config or not self.conn:
            return

        cursor = self.conn.cursor()

        # Map stream names to primary key and table names
        pk_col = 'pkt_id' if stream_name == 'sensor_stream' else \
                'event_id' if stream_name == 'event_stream' else 'cmd_id'
        ts_col = 'ts'

        print(f"[InputMonitor] Maintaining {stream_name} window: type={config.window_type}, size={config.window_size}, unit={config.window_unit}")

        try:
            if config.window_type == 'Sliding':
                if config.window_unit == 'seconds':
                    # Sliding window by time - keep last N seconds
                    del_query = f"""
                        WITH ranked_data AS (
                            SELECT 
                                {pk_col}, 
                                {ts_col},
                                MAX({ts_col}) OVER () AS max_ts
                            FROM {stream_name}
                        )
                        DELETE FROM {stream_name}
                        WHERE {pk_col} IN (
                            SELECT {pk_col}
                            FROM ranked_data
                            WHERE {ts_col} < max_ts - {config.window_size}
                        );
                    """
                    cursor.execute(del_query)
                    self.conn.commit()
                    print(f"[InputMonitor] Sliding window (time-based): Deleted old records older than {config.window_size} seconds")

                elif config.window_unit == 'packets':
                    # Sliding window by packet count - keep last N packets for sensor stream
                    del_query = f"""
                        WITH ranked_data AS (
                            SELECT 
                                {pk_col},
                                {ts_col},
                                ROW_NUMBER() OVER (ORDER BY {ts_col} DESC) AS rn
                            FROM {stream_name}
                        )
                        DELETE FROM {stream_name}
                        WHERE {pk_col} IN (
                            SELECT {pk_col}
                            FROM ranked_data
                            WHERE rn > {config.window_size}
                        );
                    """
                    cursor.execute(del_query)
                    self.conn.commit()
                    print(f"[InputMonitor] Sliding window (packet-based): Kept last {config.window_size} packets")

                elif config.window_unit == 'events':
                    # Sliding window by event count - keep last N events
                    del_query = f"""
                        WITH ranked_data AS (
                            SELECT 
                                {pk_col},
                                {ts_col},
                                ROW_NUMBER() OVER (ORDER BY {ts_col} DESC) AS rn
                            FROM {stream_name}
                        )
                        DELETE FROM {stream_name}
                        WHERE {pk_col} IN (
                            SELECT {pk_col}
                            FROM ranked_data
                            WHERE rn > {config.window_size}
                        );
                    """
                    cursor.execute(del_query)
                    self.conn.commit()
                    print(f"[InputMonitor] Sliding window (event-based): Kept last {config.window_size} events")

            elif config.window_type == 'Tumbling':
                if config.window_unit == 'seconds':
                    # Tumbling window by time - delete records outside current window
                    del_query = f"""
                        WITH ranked_data AS (
                            SELECT 
                                {pk_col}, 
                                {ts_col},
                                MAX({ts_col}) OVER () AS max_ts
                            FROM {stream_name}
                        )
                        DELETE FROM {stream_name}
                        WHERE {pk_col} IN (
                            SELECT {pk_col}
                            FROM ranked_data
                            WHERE {ts_col} < max_ts - {config.window_size}
                        );
                    """
                    cursor.execute(del_query)
                    self.conn.commit()
                    print(f"[InputMonitor] Tumbling window (time-based): Kept records within {config.window_size} seconds")

                elif config.window_unit == 'packets':
                    # Tumbling window by packet count
                    del_query = f"""
                        WITH ranked_data AS (
                            SELECT 
                                {pk_col},
                                {ts_col},
                                ROW_NUMBER() OVER (ORDER BY {ts_col} DESC) AS rn
                            FROM {stream_name}
                        )
                        DELETE FROM {stream_name}
                        WHERE {pk_col} IN (
                            SELECT {pk_col}
                            FROM ranked_data
                            WHERE rn > {config.window_size}
                        );
                    """
                    cursor.execute(del_query)
                    self.conn.commit()
                    print(f"[InputMonitor] Tumbling window (packet-based): Kept {config.window_size} packets")

                elif config.window_unit == 'events':
                    # Tumbling window by event count
                    del_query = f"""
                        WITH ranked_data AS (
                            SELECT 
                                {pk_col},
                                {ts_col},
                                ROW_NUMBER() OVER (ORDER BY {ts_col} DESC) AS rn
                            FROM {stream_name}
                        )
                        DELETE FROM {stream_name}
                        WHERE {pk_col} IN (
                            SELECT {pk_col}
                            FROM ranked_data
                            WHERE rn > {config.window_size}
                        );
                    """
                    cursor.execute(del_query)
                    self.conn.commit()
                    print(f"[InputMonitor] Tumbling window (event-based): Kept {config.window_size} events")

            elif config.window_type == 'Landmark':
                # Landmark window - keep all data from a specific point in time
                print(f"[InputMonitor] Landmark window: Keeping all data from landmark point")
                # No deletion for landmark windows
                pass

            cursor.close()

        except Exception as e:
            print(f"[InputMonitor] Error maintaining window for {stream_name}: {e}")
            cursor.close()

    def process_sensor_stream(self, xml_path, data_dir):
        print(f"[InputMonitor] Processing Sensor Stream File: {xml_path}")
        try:
            # Parse XML file first
            tree = etree.parse(xml_path)
            
            # Look for schema in input_schemas folder
            base_path = os.path.dirname(os.path.dirname(xml_path))  # Go up from data/ to project root
            schema_path = os.path.join(base_path, 'input_schemas', 'schema_sensor_input.xsd')
            
            if os.path.exists(schema_path):
                try:
                    schema_doc = etree.parse(schema_path)
                    schema = etree.XMLSchema(schema_doc)
                    schema.assertValid(tree)
                    print("[InputMonitor] sensor schema validation: SUCCESS")
                except etree.DocumentInvalid as e:
                    print(f"[InputMonitor] sensor schema validation: FAILED - {e}")
                    return
                except Exception as e:
                    print(f"[InputMonitor] Error validating sensor schema: {e}")
                    return
            else:
                print(f"[InputMonitor] Schema file not found at {schema_path}, skipping validation")
            
        except Exception as e:
            print(f"[InputMonitor] Error parsing sensor XML: {e}")
            return
            
        root = tree.getroot()
        inserted_count = 0
        max_ts = 0
        
        for event in root.findall('sensor_event'):
            event_ts = int(event.find('ts').text)
            max_ts = max(max_ts, event_ts)
            data = {
                'pkt_id': event.find('pkt_id').text,
                'intersection_id': event.find('intersection_id').text,
                'road_id': event.find('road_id').text,
                'signal_id': event.find('signal_id').text,
                'vehicle_count': int(event.find('vehicle_count').text),
                'avg_speed': float(event.find('avg_speed').text),
                'occupancy': float(event.find('occupancy').text),
                'ts': event_ts
            }
            
            if self.conn:
                query = """INSERT IGNORE INTO sensor_stream 
                           (pkt_id, intersection_id, road_id, signal_id, vehicle_count, avg_speed, occupancy, ts) 
                           VALUES (%(pkt_id)s, %(intersection_id)s, %(road_id)s, %(signal_id)s, %(vehicle_count)s, %(avg_speed)s, %(occupancy)s, %(ts)s)"""
                try:
                    self.cursor.execute(query, data)
                    self.conn.commit()
                    if self.cursor.rowcount > 0:
                        inserted_count += 1
                except Exception as e:
                    print(f"[InputMonitor] Error inserting sensor event: {e}")
            else:
                print(f"[InputMonitor] INSERT sensor_stream: {data}")
        
        if inserted_count > 0:
            self.update_tick(max_ts)
            self.maintain_window('sensor_stream')

        print(f"[InputMonitor] Inserted {inserted_count} sensor events")

    def process_event_stream(self, xml_path, data_dir):
        print(f"\n[InputMonitor] Processing Event Stream File: {xml_path}")
        try:
            # Parse XML file first
            tree = etree.parse(xml_path)
            
            # Look for schema in input_schemas folder
            base_path = os.path.dirname(os.path.dirname(xml_path))  # Go up from data/ to project root
            schema_path = os.path.join(base_path, 'input_schemas', 'schema_event_input.xsd')
            
            if os.path.exists(schema_path):
                try:
                    schema_doc = etree.parse(schema_path)
                    schema = etree.XMLSchema(schema_doc)
                    schema.assertValid(tree)
                    print("[InputMonitor] event schema validation: SUCCESS")
                except etree.DocumentInvalid as e:
                    print(f"[InputMonitor] event schema validation: FAILED - {e}")
                    return
                except Exception as e:
                    print(f"[InputMonitor] Error validating event schema: {e}")
                    return
            else:
                print(f"[InputMonitor] Schema file not found at {schema_path}, skipping validation")
            
        except Exception as e:
            print(f"[InputMonitor] Error parsing event XML: {e}")
            return
            
        root = tree.getroot()
        inserted_count = 0
        max_ts = 0
        
        for event in root.findall('event'):
            event_ts = int(event.find('ts').text)
            max_ts = max(max_ts, event_ts)
            data = {
                'event_id': event.find('event_id').text,
                'event_type': event.find('event_type').text,
                'intersection_id': event.find('intersection_id').text,
                'road_id': event.find('road_id').text,
                'priority': int(event.find('priority').text),
                'ts': event_ts
            }
            
            if self.conn:
                query = """INSERT IGNORE INTO event_stream 
                           (event_id, event_type, intersection_id, road_id, priority, ts) 
                           VALUES (%(event_id)s, %(event_type)s, %(intersection_id)s, %(road_id)s, %(priority)s, %(ts)s)"""
                self.cursor.execute(query, data)
                self.conn.commit()
                if self.cursor.rowcount > 0:
                    inserted_count += 1
            else:
                print(f"INSERT event_stream: {data}")

        if inserted_count > 0:
            self.update_tick(max_ts)
            self.maintain_window('event_stream')

        print(f"[InputMonitor] Inserted {inserted_count} events")

    def process_command_stream(self, xml_path, data_dir):
        print(f"\n[InputMonitor] Processing Command Stream File: {xml_path}")
        try:
            # Parse XML file first
            tree = etree.parse(xml_path)
            
            # Look for schema in input_schemas folder
            base_path = os.path.dirname(os.path.dirname(xml_path))  # Go up from data/ to project root
            schema_path = os.path.join(base_path, 'input_schemas', 'schema_command_input.xsd')
            
            if os.path.exists(schema_path):
                try:
                    schema_doc = etree.parse(schema_path)
                    schema = etree.XMLSchema(schema_doc)
                    schema.assertValid(tree)
                    print("[InputMonitor] command schema validation: SUCCESS")
                except etree.DocumentInvalid as e:
                    print(f"[InputMonitor] command schema validation: FAILED - {e}")
                    return
                except Exception as e:
                    print(f"[InputMonitor] Error validating command schema: {e}")
                    return
            else:
                print(f"[InputMonitor] Schema file not found at {schema_path}, skipping validation")
            
        except Exception as e:
            print(f"[InputMonitor] Error parsing command XML: {e}")
            return
            
        root = tree.getroot()
        inserted_count = 0
        max_ts = 0
        
        for event in root.findall('command'):
            event_ts = int(event.find('ts').text)
            max_ts = max(max_ts, event_ts)
            data = {
                'cmd_id': event.find('cmd_id').text,
                'signal_id': event.find('signal_id').text,
                'action': event.find('action').text,
                'duration': int(event.find('duration').text),
                'reason': event.find('reason').text,
                'ts': event_ts
            }
            
            if self.conn:
                query = """INSERT IGNORE INTO command_stream 
                           (cmd_id, signal_id, action, duration, reason, ts) 
                           VALUES (%(cmd_id)s, %(signal_id)s, %(action)s, %(duration)s, %(reason)s, %(ts)s)"""
                self.cursor.execute(query, data)
                self.conn.commit()
                if self.cursor.rowcount > 0:
                    inserted_count += 1
            else:
                print(f"INSERT command_stream: {data}")

        if inserted_count > 0:
            self.update_tick(max_ts)
            self.maintain_window('command_stream')

        print(f"[InputMonitor] Inserted {inserted_count} commands")

    def run(self, data_dir, base_path):
        sensor_file = os.path.join(data_dir, 'dummy_sensor.xml')
        event_file = os.path.join(data_dir, 'dummy_event.xml')
        command_file = os.path.join(data_dir, 'dummy_command.xml')
        
        sensor_xsd = os.path.join(base_path, 'schema_sensor_input.xsd')
        event_xsd = os.path.join(base_path, 'schema_event_input.xsd')
        command_xsd = os.path.join(base_path, 'schema_command_input.xsd')

        if os.path.exists(sensor_file):
            self.process_sensor_stream(sensor_file, data_dir)
        if os.path.exists(event_file):
            self.process_event_stream(event_file, data_dir)
        if os.path.exists(command_file):
            self.process_command_stream(command_file, data_dir)

        if self.conn:
            self.cursor.close()
            self.conn.close()
            print("\nClosed database connection.")

if __name__ == "__main__":
    import time
    import hashlib
    
    import json
    
    with open('config.json', 'r') as f:
        config_file = json.load(f)
    DB_CONFIG = config_file['database']

    base_path = '.'
    traffic_config = os.path.join(base_path, 'configs', 'traffic.xml')
    data_dir = os.path.join(base_path, 'data')

    monitor = InputMonitor(DB_CONFIG)
    
    # 1. Parse rules from traffic.xml
    monitor.parse_traffic_config(traffic_config)
    
    # 2. Connect to DB (If mysql isn't running, it will gracefully fallback to mock/print output mode)
    monitor.connect()
    
    print("[InputMonitor] Starting InputMonitor - Listening for XML files in:", data_dir)
    print("[InputMonitor] Press Ctrl+C to stop monitoring.\n")
    
    # Track file hashes to detect when files are updated
    file_hashes = {
        'sensor': None,
        'event': None,
        'command': None
    }
    
    def get_file_hash(file_path):
        """Get MD5 hash of file to detect changes"""
        if not os.path.exists(file_path):
            return None
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except:
            return None
    
    try:
        while True:
            sensor_file = os.path.join(data_dir, 'dummy_sensor.xml')
            event_file = os.path.join(data_dir, 'dummy_event.xml')
            command_file = os.path.join(data_dir, 'dummy_command.xml')
            
            # Check if sensor file has changed
            sensor_hash = get_file_hash(sensor_file)
            if sensor_hash and sensor_hash != file_hashes['sensor']:
                try:
                    print(f"[InputMonitor] Detected new sensor stream file")
                    monitor.process_sensor_stream(sensor_file, data_dir)
                    monitor.maintain_window('sensor_stream')
                    file_hashes['sensor'] = sensor_hash
                except Exception as e:
                    print(f"[InputMonitor] Error processing sensor stream: {e}")
            
            # Check if event file has changed
            event_hash = get_file_hash(event_file)
            if event_hash and event_hash != file_hashes['event']:
                try:
                    print(f"[InputMonitor] Detected new event stream file")
                    monitor.process_event_stream(event_file, data_dir)
                    monitor.maintain_window('event_stream')
                    file_hashes['event'] = event_hash
                except Exception as e:
                    print(f"[InputMonitor] Error processing event stream: {e}")
            
            # Check if command file has changed
            command_hash = get_file_hash(command_file)
            if command_hash and command_hash != file_hashes['command']:
                try:
                    print(f"[InputMonitor] Detected new command stream file")
                    monitor.process_command_stream(command_file, data_dir)
                    monitor.maintain_window('command_stream')
                    file_hashes['command'] = command_hash
                except Exception as e:
                    print(f"[InputMonitor] Error processing command stream: {e}")
            
            # Sleep briefly before checking again
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("[InputMonitor] Input monitoring stopped by user.")
        if monitor.conn:
            monitor.cursor.close()
            monitor.conn.close()
            print("[InputMonitor] Database connection closed.")
