from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import mysql.connector
import json
import subprocess
import threading
import time
import psutil
import os
import xml.etree.ElementTree as ET
from query_builder import QueryConfigError, build_sql

app = Flask(__name__, template_folder='frontend', static_folder='frontend', static_url_path='')
CORS(app)

import json

with open('config.json', 'r') as f:
    config_file = json.load(f)
DB_CONFIG = config_file['database']

generator_process = None
processor_process = None
monitor_process = None

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_FILES = (
    os.path.join(BASE_PATH, 'data', 'dummy_sensor.xml'),
    os.path.join(BASE_PATH, 'data', 'dummy_event.xml'),
    os.path.join(BASE_PATH, 'data', 'dummy_command.xml'),
)
TRAFFIC_CONFIG_PATH = os.path.join(BASE_PATH, 'configs', 'traffic.xml')
STREAM_NS = 'http://sdms/stream'
WINDOW_TYPES = {'Sliding', 'Tumbling', 'Landmark'}
WINDOW_UNITS = {'seconds', 'packets', 'events', ''}

QUERY_SOURCES = {
    'sensor_stream': ['pkt_id', 'intersection_id', 'road_id', 'signal_id', 'vehicle_count', 'avg_speed', 'occupancy', 'ts'],
    'event_stream': ['event_id', 'event_type', 'intersection_id', 'road_id', 'priority', 'ts'],
    'command_stream': ['cmd_id', 'signal_id', 'action', 'duration', 'reason', 'ts'],
}

DEFAULT_QUERY_DEFINITIONS = [
    {
        'query_id': 'SENSOR-METRICS',
        'description': 'Ready sensor query: add any sensor field and aggregate pairs here',
        'frequency_sec': 1,
        'query_config': {
            'query_id': 'SENSOR-METRICS',
            'source': 'sensor_stream',
            'filter': {'field': 'ts', 'operator': '=', 'value': '{CURRENT_TICK}'},
            'group_by': ['road_id', 'intersection_id'],
            'dimension_fields': ['road_id', 'intersection_id'],
            'aggregations': [
                {'name': 'avg_speed_avg', 'operation': 'avg', 'field': 'avg_speed'}
            ]
        }
    },
    {
        'query_id': 'EVENT-METRICS',
        'description': 'Ready event query: add any event field and aggregate pairs here',
        'frequency_sec': 1,
        'query_config': {
            'query_id': 'EVENT-METRICS',
            'source': 'event_stream',
            'filter': {'field': 'ts', 'operator': '=', 'value': '{CURRENT_TICK}'},
            'group_by': ['road_id', 'intersection_id'],
            'dimension_fields': ['road_id', 'intersection_id'],
            'aggregations': [
                {'name': 'event_type_count', 'operation': 'count', 'field': 'event_type'},
                {'name': 'priority_max', 'operation': 'max', 'field': 'priority'}
            ]
        }
    }
]

def normalize_query_config(query_id, data):
    query_config = data.get('query_config')
    if not isinstance(query_config, dict):
        query_config = {
            'query_id': query_id,
            'source': data.get('source'),
            'filter': data.get('filter') or {'field': 'ts', 'operator': '=', 'value': '{CURRENT_TICK}'},
            'group_by': data.get('group_by') or [],
            'aggregations': data.get('aggregations') or []
        }

    query_config['query_id'] = query_id
    if query_id == 'SENSOR-METRICS':
        query_config['source'] = 'sensor_stream'
    elif query_id == 'EVENT-METRICS':
        query_config['source'] = 'event_stream'
    if query_config.get('source') == 'sensor_stream':
        query_config['group_by'] = ['road_id', 'intersection_id']
        query_config['dimension_fields'] = ['road_id', 'intersection_id']
    elif query_config.get('source') == 'event_stream':
        query_config['group_by'] = ['road_id', 'intersection_id']
        query_config['dimension_fields'] = ['road_id', 'intersection_id']
    for agg in query_config.get('aggregations', []):
        if isinstance(agg, dict) and query_config.get('source') == 'sensor_stream' and agg.get('field') == 'speed':
            agg['field'] = 'avg_speed'
        if isinstance(agg, dict):
            field = agg.get('field') or '*'
            operation = agg.get('operation') or 'count'
            safe_field = 'all' if field == '*' else field
            agg['name'] = f"{safe_field}_{operation}"
    build_sql(query_config, 0)
    return query_config

def log_process_output(process, name):
    """Continuously log process output in background"""
    def log_stream(stream, prefix):
        for line in iter(stream.readline, ''):
            if line:
                print(f"[{prefix}] {line.rstrip()}")
    
    if process and process.stdout:
        threading.Thread(target=log_stream, args=(process.stdout, f"{name}-OUT"), daemon=True).start()
    if process and process.stderr:
        threading.Thread(target=log_stream, args=(process.stderr, f"{name}-ERR"), daemon=True).start()

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except:
        return None

def ensure_query_repository_schema(conn):
    """Upgrade existing databases from query_sql to query_config without a full rebuild."""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SHOW COLUMNS FROM query_repository")
    columns = {row['Field'] for row in cursor.fetchall()}

    if 'query_config' not in columns:
        cursor.execute("ALTER TABLE query_repository ADD COLUMN query_config JSON AFTER description")
        print("[DEBUG] Added query_repository.query_config column")

    cursor.close()
    conn.commit()

def ensure_default_query_configs(conn):
    ensure_query_repository_schema(conn)
    cursor = conn.cursor()
    default_ids = [item['query_id'] for item in DEFAULT_QUERY_DEFINITIONS]
    cursor.execute(
        f"DELETE FROM query_repository WHERE query_id NOT IN ({', '.join(['%s'] * len(default_ids))})",
        tuple(default_ids)
    )
    for item in DEFAULT_QUERY_DEFINITIONS:
        cursor.execute(
            """
            INSERT INTO query_repository (query_id, description, query_config, frequency_sec, last_run)
            VALUES (%s, %s, %s, %s, 0)
            ON DUPLICATE KEY UPDATE
                description = VALUES(description),
                query_config = COALESCE(query_config, VALUES(query_config)),
                frequency_sec = COALESCE(frequency_sec, VALUES(frequency_sec))
            """,
            (
                item['query_id'],
                item['description'],
                json.dumps(item['query_config']),
                item['frequency_sec']
            )
        )
    cursor.close()
    conn.commit()

def ensure_summary_state_schema(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS summary_metric_values (
            stream_name VARCHAR(30),
            metric_field VARCHAR(50),
            aggregate_name VARCHAR(20),
            dimensions VARCHAR(255),
            value DOUBLE DEFAULT 0,
            helper_count DOUBLE DEFAULT 0,
            last_updated_tick INT,
            PRIMARY KEY (stream_name, metric_field, aggregate_name, dimensions)
        )
    """)
    cursor.close()
    conn.commit()

def clear_generated_input_files():
    """Remove generated XML batches so reset/start cannot re-import stale stream data."""
    for file_path in DATA_FILES:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[DEBUG] Removed stale input file: {file_path}")
        except Exception as e:
            print(f"[DEBUG] Could not remove stale input file {file_path}: {e}")

def ns_tag(name):
    return f'{{{STREAM_NS}}}{name}'

def find_child(parent, name):
    child = parent.find(ns_tag(name))
    if child is not None:
        return child
    return parent.find(name)

def ensure_child(parent, name):
    child = find_child(parent, name)
    if child is None:
        child = ET.SubElement(parent, ns_tag(name))
    return child

def remove_child(parent, name):
    child = find_child(parent, name)
    if child is not None:
        parent.remove(child)

def read_window_settings():
    tree = ET.parse(TRAFFIC_CONFIG_PATH)
    root = tree.getroot()
    streams = []

    for stream_el in root.findall(f'.//{ns_tag("stream")}'):
        name_el = find_child(stream_el, 'name')
        if name_el is None or not name_el.text:
            continue

        window_el = find_child(stream_el, 'window')
        window_type = ''
        size = ''
        unit = ''

        if window_el is not None:
            type_el = find_child(window_el, 'windowType')
            size_el = find_child(window_el, 'size')
            unit_el = find_child(window_el, 'unit')
            window_type = type_el.text.strip() if type_el is not None and type_el.text else ''
            size = size_el.text.strip() if size_el is not None and size_el.text else ''
            unit = unit_el.text.strip() if unit_el is not None and unit_el.text else ''

        streams.append({
            'name': name_el.text.strip(),
            'windowType': window_type,
            'size': size,
            'unit': unit
        })

    return streams

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/system/start', methods=['POST'])
def start_system():
    global generator_process, processor_process, monitor_process
    
    try:
        print("[DEBUG] Starting system...")
        
        # Kill any existing processes
        import psutil
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline', [])
                if cmdline and 'python' in proc.info.get('name', ''):
                    cmd_str = ' '.join(cmdline)
                    if 'input_generator.py' in cmd_str or 'query_processor.py' in cmd_str or 'input_monitor.py' in cmd_str:
                        print(f"[DEBUG] Killing stray process: {cmd_str} (PID: {proc.pid})")
                        proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Kill existing process objects if any
        if generator_process:
            try:
                print(f"[DEBUG] Killing old generator process (PID: {generator_process.pid})")
                generator_process.kill()
                generator_process.wait(timeout=2)
            except:
                pass
            generator_process = None
            
        if processor_process:
            try:
                print(f"[DEBUG] Killing old processor process (PID: {processor_process.pid})")
                processor_process.kill()
                processor_process.wait(timeout=2)
            except:
                pass
            processor_process = None
                
        # Reset database
        conn = get_db_connection()
        if conn:
            ensure_default_query_configs(conn)
            ensure_summary_state_schema(conn)
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE sensor_stream")
            cursor.execute("TRUNCATE TABLE event_stream")
            cursor.execute("TRUNCATE TABLE command_stream")
            cursor.execute("TRUNCATE TABLE output_buffer")
            cursor.execute("TRUNCATE TABLE summary_metric_values")
            # Ensure system_tick row exists and reset to 0
            cursor.execute("DELETE FROM system_tick WHERE id = 1")
            cursor.execute("INSERT INTO system_tick (id, current_tick) VALUES (1, 0)")
            cursor.execute("UPDATE query_repository SET last_run = 0")
            conn.commit()
            cursor.close()
            conn.close()
            print("[DEBUG] Database reset for new system start - Tick set to 0")

        clear_generated_input_files()
        
        # Start query processor
        import sys
        python_exec = sys.executable
        processor_process = subprocess.Popen(
            [python_exec, 'query_processor.py'],
            cwd=BASE_PATH,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        print(f"[DEBUG] Query processor started with PID: {processor_process.pid}")
        log_process_output(processor_process, "QueryProcessor")
        
        time.sleep(1)
        
        # Start input generator
        generator_process = subprocess.Popen(
            [python_exec, 'input_generator.py'],
            cwd=BASE_PATH,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        print(f"[DEBUG] Input generator started with PID: {generator_process.pid}")
        log_process_output(generator_process, "InputGenerator")
        
        time.sleep(1)
        
        # Start input monitor (watches and processes XML files)
        monitor_process = subprocess.Popen(
            [python_exec, 'input_monitor.py'],
            cwd=BASE_PATH,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        print(f"[DEBUG] Input monitor started with PID: {monitor_process.pid}")
        log_process_output(monitor_process, "InputMonitor")
        
        # Check if processes started successfully
        time.sleep(0.5)
        gen_poll = generator_process.poll()
        proc_poll = processor_process.poll()
        mon_poll = monitor_process.poll()
        
        if gen_poll is not None:
            try:
                _, stderr_output = generator_process.communicate(timeout=1)
            except:
                stderr_output = "Failed to read stderr"
            print(f"[ERROR] Generator process exited immediately with code {gen_poll}: {stderr_output}")
            generator_process = None
            return jsonify({'status': 'error', 'message': f'Generator process failed: {stderr_output}'}), 500
        
        if proc_poll is not None:
            try:
                _, stderr_output = processor_process.communicate(timeout=1)
            except:
                stderr_output = "Failed to read stderr"
            print(f"[ERROR] Processor process exited immediately with code {proc_poll}: {stderr_output}")
            processor_process = None
            return jsonify({'status': 'error', 'message': f'Processor process failed: {stderr_output}'}), 500
        
        if mon_poll is not None:
            try:
                _, stderr_output = monitor_process.communicate(timeout=1)
            except:
                stderr_output = "Failed to read stderr"
            print(f"[ERROR] Monitor process exited immediately with code {mon_poll}: {stderr_output}")
            monitor_process = None
            return jsonify({'status': 'error', 'message': f'Monitor process failed: {stderr_output}'}), 500
        
        print("[DEBUG] System started successfully")
        return jsonify({'status': 'success', 'message': 'System started successfully'})
    except Exception as e:
        print(f"[ERROR] in start_system: {str(e)}")
        import traceback
        traceback.print_exc()
        generator_process = None
        processor_process = None
        monitor_process = None
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system/sensors', methods=['POST'])
def update_active_sensors():
    data = request.json
    sensors = data.get('sensors', [])
    try:
        with open('active_sensors.json', 'w') as f:
            json.dump(sensors, f)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/stop', methods=['POST'])
def stop_system():
    global generator_process, processor_process, monitor_process
    
    try:
        print("[DEBUG] Stopping system...")
        
        # Kill by PID using psutil for more reliable termination
        if generator_process:
            try:
                print(f"[DEBUG] Killing generator process (PID: {generator_process.pid})")
                generator_process.terminate()
                try:
                    generator_process.wait(timeout=2)
                except:
                    generator_process.kill()
                    generator_process.wait(timeout=2)
            except Exception as e:
                print(f"[DEBUG] Error killing generator: {e}")
            generator_process = None
            
        if processor_process:
            try:
                print(f"[DEBUG] Killing processor process (PID: {processor_process.pid})")
                processor_process.terminate()
                try:
                    processor_process.wait(timeout=2)
                except:
                    processor_process.kill()
                    processor_process.wait(timeout=2)
            except Exception as e:
                print(f"[DEBUG] Error killing processor: {e}")
            processor_process = None
        
        if monitor_process:
            try:
                print(f"[DEBUG] Killing monitor process (PID: {monitor_process.pid})")
                monitor_process.terminate()
                try:
                    monitor_process.wait(timeout=2)
                except:
                    monitor_process.kill()
                    monitor_process.wait(timeout=2)
            except Exception as e:
                print(f"[DEBUG] Error killing monitor: {e}")
            monitor_process = None
        
        print("[DEBUG] System stopped successfully")
        return jsonify({'status': 'success', 'message': 'System stopped'})
    except Exception as e:
        print(f"[ERROR] in stop_system: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system/reset', methods=['POST'])
def reset_system():
    """Reset all streams and system state"""
    global generator_process, processor_process, monitor_process
    
    try:
        print("[DEBUG] Resetting system...")
        
        # First, stop any running processes
        if generator_process:
            try:
                print(f"[DEBUG] Killing generator process for reset (PID: {generator_process.pid})")
                generator_process.terminate()
                try:
                    generator_process.wait(timeout=2)
                except:
                    generator_process.kill()
                    generator_process.wait(timeout=2)
            except Exception as e:
                print(f"[DEBUG] Error killing generator: {e}")
            generator_process = None
            
        if processor_process:
            try:
                print(f"[DEBUG] Killing processor process for reset (PID: {processor_process.pid})")
                processor_process.terminate()
                try:
                    processor_process.wait(timeout=2)
                except:
                    processor_process.kill()
                    processor_process.wait(timeout=2)
            except Exception as e:
                print(f"[DEBUG] Error killing processor: {e}")
            processor_process = None
        
        if monitor_process:
            try:
                print(f"[DEBUG] Killing monitor process for reset (PID: {monitor_process.pid})")
                monitor_process.terminate()
                try:
                    monitor_process.wait(timeout=2)
                except:
                    monitor_process.kill()
                    monitor_process.wait(timeout=2)
            except Exception as e:
                print(f"[DEBUG] Error killing monitor: {e}")
            monitor_process = None

        # Reset database
        conn = get_db_connection()
        if conn:
            ensure_default_query_configs(conn)
            ensure_summary_state_schema(conn)
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE sensor_stream")
            cursor.execute("TRUNCATE TABLE event_stream")
            cursor.execute("TRUNCATE TABLE command_stream")
            cursor.execute("TRUNCATE TABLE output_buffer")
            cursor.execute("TRUNCATE TABLE summary_metric_values")
            # Ensure system_tick row exists and reset to 0
            cursor.execute("DELETE FROM system_tick WHERE id = 1")
            cursor.execute("INSERT INTO system_tick (id, current_tick) VALUES (1, 0)")
            cursor.execute("UPDATE query_repository SET last_run = 0")
            conn.commit()
            cursor.close()
            conn.close()
            print("[DEBUG] Database reset successfully - Tick set to 0")

        clear_generated_input_files()
        
        print("[DEBUG] System reset successfully")
        return jsonify({'status': 'success', 'message': 'System reset successfully'})
    except Exception as e:
        print(f"[ERROR] in reset_system: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system/status', methods=['GET'])
def system_status():
    global generator_process, processor_process, monitor_process
    
    gen_running = generator_process and generator_process.poll() is None
    proc_running = processor_process and processor_process.poll() is None
    mon_running = monitor_process and monitor_process.poll() is None
    
    print(f"[DEBUG] Status check - Generator: {gen_running} (PID: {generator_process.pid if generator_process else 'None'}), Processor: {proc_running} (PID: {processor_process.pid if processor_process else 'None'}), Monitor: {mon_running} (PID: {monitor_process.pid if monitor_process else 'None'})")
    
    return jsonify({
        'generator_running': gen_running,
        'processor_running': proc_running,
        'monitor_running': mon_running,
        'system_running': gen_running and proc_running and mon_running
    })



@app.route('/api/sensors', methods=['GET'])
def api_sensors():
    """
    Read configs/traffic.xml and return the <stream><name> (and optional <description>)
    as JSON array: { streams: [{ name: "...", description: "..." }, ...] }
    """
    cfg_path = os.path.join(os.path.dirname(__file__), 'configs', 'traffic.xml')
    if not os.path.exists(cfg_path):
        return jsonify({'error': 'configs/traffic.xml not found', 'streams': []}), 404

    try:
        tree = ET.parse(cfg_path)
        root = tree.getroot()
        streams = []

        # find all <stream> elements regardless of namespace
        ns = {'sdms': 'http://sdms/stream'}
        for stream_el in root.findall('.//sdms:stream', ns):
            name_el = stream_el.find('sdms:name', ns)
            if name_el is None:
                for child in stream_el:
                    if child.tag.endswith('name'):
                        name_el = child
                        break
            desc_el = stream_el.find('sdms:description', ns)
            if desc_el is None:
                for child in stream_el:
                    if child.tag.endswith('description'):
                        desc_el = child
                        break

            if name_el is not None and (name_el.text and name_el.text.strip()):
                name = name_el.text.strip()
                desc = (desc_el.text.strip() if (desc_el is not None and desc_el.text) else '')
                streams.append({'name': name, 'description': desc})

        return jsonify({'streams': streams})
    except ET.ParseError as e:
        return jsonify({'error': 'XML parse error', 'detail': str(e), 'streams': []}), 500
    except Exception as e:
        return jsonify({'error': str(e), 'streams': []}), 500

@app.route('/api/settings/windows', methods=['GET'])
def get_window_settings():
    try:
        if not os.path.exists(TRAFFIC_CONFIG_PATH):
            return jsonify({'error': 'configs/traffic.xml not found', 'streams': []}), 404
        return jsonify({'streams': read_window_settings()})
    except ET.ParseError as e:
        return jsonify({'error': 'XML parse error', 'detail': str(e), 'streams': []}), 500
    except Exception as e:
        return jsonify({'error': str(e), 'streams': []}), 500

@app.route('/api/settings/windows', methods=['POST'])
def update_window_settings():
    try:
        data = request.json or {}
        updates = data.get('streams', [])
        if not isinstance(updates, list):
            return jsonify({'status': 'error', 'message': 'streams must be a list'}), 400

        update_map = {}
        for item in updates:
            if not isinstance(item, dict):
                return jsonify({'status': 'error', 'message': 'each stream setting must be an object'}), 400

            name = str(item.get('name', '')).strip()
            window_type = str(item.get('windowType', '')).strip()
            unit = str(item.get('unit', '')).strip()
            raw_size = item.get('size', '')

            if not name:
                return jsonify({'status': 'error', 'message': 'stream name is required'}), 400
            if window_type not in WINDOW_TYPES:
                return jsonify({'status': 'error', 'message': f'invalid window type for {name}'}), 400
            if unit not in WINDOW_UNITS:
                return jsonify({'status': 'error', 'message': f'invalid window unit for {name}'}), 400

            size = ''
            if raw_size not in ('', None):
                try:
                    size = int(raw_size)
                except (TypeError, ValueError):
                    return jsonify({'status': 'error', 'message': f'window size for {name} must be a number'}), 400
                if size <= 0:
                    return jsonify({'status': 'error', 'message': f'window size for {name} must be greater than 0'}), 400

            update_map[name] = {
                'windowType': window_type,
                'size': size,
                'unit': unit
            }

        if not os.path.exists(TRAFFIC_CONFIG_PATH):
            return jsonify({'status': 'error', 'message': 'configs/traffic.xml not found'}), 404

        ET.register_namespace('', STREAM_NS)
        tree = ET.parse(TRAFFIC_CONFIG_PATH)
        root = tree.getroot()
        updated = []

        for stream_el in root.findall(f'.//{ns_tag("stream")}'):
            name_el = find_child(stream_el, 'name')
            if name_el is None or not name_el.text:
                continue

            name = name_el.text.strip()
            if name not in update_map:
                continue

            settings = update_map[name]
            window_el = ensure_child(stream_el, 'window')
            ensure_child(window_el, 'windowType').text = settings['windowType']

            if settings['size'] == '':
                remove_child(window_el, 'size')
            else:
                ensure_child(window_el, 'size').text = str(settings['size'])

            if settings['unit'] == '':
                remove_child(window_el, 'unit')
            else:
                ensure_child(window_el, 'unit').text = settings['unit']

            updated.append(name)

        missing = sorted(set(update_map.keys()) - set(updated))
        if missing:
            return jsonify({'status': 'error', 'message': f'unknown stream(s): {", ".join(missing)}'}), 400

        ET.indent(tree, space="    ")
        tree.write(TRAFFIC_CONFIG_PATH, encoding='UTF-8', xml_declaration=True)

        return jsonify({'status': 'success', 'streams': read_window_settings()})
    except ET.ParseError as e:
        return jsonify({'status': 'error', 'message': 'XML parse error', 'detail': str(e)}), 500
    except Exception as e:
        print(f"[ERROR] Error updating window settings: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/data/tick', methods=['GET'])
def get_current_tick():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'tick': 0})
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT current_tick FROM system_tick WHERE id = 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({'tick': result['current_tick'] if result else 0})
    except:
        return jsonify({'tick': 0})

@app.route('/api/data/streams', methods=['GET'])
def get_stream_data():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({
                'sensor_count': 0,
                'event_count': 0,
                'command_count': 0
            })
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT COUNT(*) as cnt FROM sensor_stream")
        sensor_count = cursor.fetchone()['cnt']
        
        cursor.execute("SELECT COUNT(*) as cnt FROM event_stream")
        event_count = cursor.fetchone()['cnt']
        
        cursor.execute("SELECT COUNT(*) as cnt FROM command_stream")
        command_count = cursor.fetchone()['cnt']
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'sensor_count': sensor_count,
            'event_count': event_count,
            'command_count': command_count
        })
    except:
        return jsonify({
            'sensor_count': 0,
            'event_count': 0,
            'command_count': 0
        })

@app.route('/api/data/output-buffer', methods=['GET'])
def get_output_buffer():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify([])
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT id, query_id, ts, result 
            FROM output_buffer 
            ORDER BY ts ASC, id ASC
        """)
        results = cursor.fetchall()
        
        # Parse JSON results
        for result in results:
            try:
                result['parsed_result'] = json.loads(result['result'])
            except:
                result['parsed_result'] = []
        
        cursor.close()
        conn.close()
        
        return jsonify(results)
    except Exception as e:
        return jsonify([])

@app.route('/api/data/sensor-table', methods=['GET'])
def get_sensor_table():
    try:
        conn = get_db_connection()
        if not conn:
            print("[ERROR] sensor-table: Could not connect to database")
            return jsonify({'rows': [], 'count': 0})
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT pkt_id, ts, intersection_id, road_id, avg_speed as speed, occupancy 
            FROM sensor_stream 
            ORDER BY ts DESC 
            LIMIT 1000
        """)
        rows = cursor.fetchall()
        
        cursor.execute("SELECT COUNT(*) as cnt FROM sensor_stream")
        count = cursor.fetchone()['cnt']
        
        cursor.close()
        conn.close()
        
        print(f"[DEBUG] sensor-table: Returning {count} rows")
        return jsonify({'rows': rows, 'count': count})
    except Exception as e:
        print(f"[ERROR] sensor-table: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'rows': [], 'count': 0})

@app.route('/api/data/event-table', methods=['GET'])
def get_event_table():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'rows': [], 'count': 0})
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT event_id as evt_id, ts, road_id, event_type 
            FROM event_stream 
            ORDER BY ts DESC 
            LIMIT 1000
        """)
        rows = cursor.fetchall()
        
        cursor.execute("SELECT COUNT(*) as cnt FROM event_stream")
        count = cursor.fetchone()['cnt']
        
        cursor.close()
        conn.close()
        
        return jsonify({'rows': rows, 'count': count})
    except Exception as e:
        return jsonify({'rows': [], 'count': 0})

@app.route('/api/data/command-table', methods=['GET'])
def get_command_table():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'rows': [], 'count': 0})
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT cmd_id, ts, signal_id as intersection_id, action, reason
            FROM command_stream 
            ORDER BY ts DESC 
            LIMIT 1000
        """)
        rows = cursor.fetchall()
        
        cursor.execute("SELECT COUNT(*) as cnt FROM command_stream")
        count = cursor.fetchone()['cnt']
        
        cursor.close()
        conn.close()
        
        return jsonify({'rows': rows, 'count': count})
    except Exception as e:
        return jsonify({'rows': [], 'count': 0})

@app.route('/api/data/window-stats', methods=['GET'])
def get_window_stats():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({})
        
        cursor = conn.cursor(dictionary=True)
        
        # Get window configurations
        cursor.execute("""
            SELECT * FROM query_repository LIMIT 3
        """)
        queries = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'queries': queries
        })
    except:
        return jsonify({})

@app.route('/api/queries', methods=['GET'])
def get_all_queries():
    """Get all queries from query_repository"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'queries': [], 'error': 'Database connection failed'}), 500
        ensure_default_query_configs(conn)
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT query_id, description, query_config, frequency_sec FROM query_repository ORDER BY query_id")
        queries = cursor.fetchall()
        for query in queries:
            if query.get('query_config') and isinstance(query['query_config'], str):
                query['query_config'] = json.loads(query['query_config'])
        cursor.close()
        conn.close()
        
        print(f"[DEBUG] Retrieved {len(queries)} queries from repository")
        return jsonify({'queries': queries})
    except Exception as e:
        print(f"[ERROR] Error retrieving queries: {e}")
        return jsonify({'queries': [], 'error': str(e)}), 500

@app.route('/api/queries/metadata', methods=['GET'])
def get_query_metadata():
    return jsonify({
        'sources': [{'name': name, 'fields': fields} for name, fields in QUERY_SOURCES.items()],
        'operations': ['none', 'sum', 'count', 'avg', 'min', 'max'],
        'operators': ['=', '!=', '<>', '>', '>=', '<', '<=']
    })

@app.route('/api/queries', methods=['POST'])
def add_query():
    """Add a new query to query_repository"""
    try:
        data = request.json or {}
        query_id = data.get('query_id', '').strip()
        description = data.get('description', '').strip()
        frequency_sec = int(data.get('frequency_sec', 30))
        
        if not query_id:
            return jsonify({'status': 'error', 'message': 'query_id is required'}), 400
        if frequency_sec <= 0:
            return jsonify({'status': 'error', 'message': 'frequency_sec must be greater than 0'}), 400

        try:
            query_config = normalize_query_config(query_id, data)
        except QueryConfigError as e:
            return jsonify({'status': 'error', 'message': str(e)}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        ensure_query_repository_schema(conn)
        
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO query_repository (query_id, description, query_config, frequency_sec, last_run) VALUES (%s, %s, %s, %s, 0)",
            (query_id, description, json.dumps(query_config), frequency_sec)
        )
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"[DEBUG] Added new query: {query_id}")
        return jsonify({'status': 'success', 'message': f'Query {query_id} added successfully'})
    except Exception as e:
        print(f"[ERROR] Error adding query: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/queries/<query_id>', methods=['PUT'])
def update_query(query_id):
    """Update an existing query in query_repository"""
    try:
        data = request.json or {}
        description = data.get('description', '').strip()

        try:
            frequency_sec = int(data.get('frequency_sec', 30))
        except (TypeError, ValueError):
            return jsonify({'status': 'error', 'message': 'frequency_sec must be a number'}), 400

        if frequency_sec <= 0:
            return jsonify({'status': 'error', 'message': 'frequency_sec must be greater than 0'}), 400
        try:
            query_config = normalize_query_config(query_id, data)
        except QueryConfigError as e:
            return jsonify({'status': 'error', 'message': str(e)}), 400

        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        ensure_query_repository_schema(conn)

        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE query_repository
            SET description = %s, query_config = %s, frequency_sec = %s, last_run = 0
            WHERE query_id = %s
            """,
            (description, json.dumps(query_config), frequency_sec, query_id)
        )

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'status': 'error', 'message': f'Query {query_id} not found'}), 404

        source = query_config.get('source')
        if source in ('sensor_stream', 'event_stream'):
            cursor.execute("DELETE FROM summary_metric_values WHERE stream_name = %s", (source,))
            cursor.execute("DELETE FROM output_buffer WHERE query_id = %s", (query_id,))

        conn.commit()

        cursor.close()
        conn.close()

        print(f"[DEBUG] Updated query: {query_id}")
        return jsonify({'status': 'success', 'message': f'Query {query_id} updated successfully'})
    except Exception as e:
        print(f"[ERROR] Error updating query: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/graphs/output-buffer', methods=['GET'])
def get_output_buffer_graph():
    """Build graph points by parsing output_buffer JSON results."""
    try:
        query_id = request.args.get('query_id', '').strip()
        y_field = request.args.get('y_field', '').strip()
        filter_field = request.args.get('filter_field', '').strip()
        filter_value = request.args.get('filter_value', '').strip()

        try:
            window = int(request.args.get('window', 60))
        except (TypeError, ValueError):
            window = 60

        if not query_id:
            return jsonify({'status': 'error', 'message': 'query_id is required'}), 400
        if not y_field:
            return jsonify({'status': 'error', 'message': 'y_field is required'}), 400
        if window <= 0:
            window = 60

        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500

        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT MAX(ts) AS max_ts FROM output_buffer WHERE query_id = %s", (query_id,))
        max_row = cursor.fetchone()
        max_ts = max_row['max_ts'] if max_row else None

        if max_ts is None:
            cursor.close()
            conn.close()
            return jsonify({
                'status': 'success',
                'query_id': query_id,
                'y_field': y_field,
                'filter_field': filter_field,
                'filter_value': filter_value,
                'window': window,
                'points': [],
                'fields': []
            })

        start_ts = max(0, int(max_ts) - window)
        cursor.execute(
            """
            SELECT ts, result
            FROM output_buffer
            WHERE query_id = %s AND ts >= %s
            ORDER BY ts ASC, id ASC
            """,
            (query_id, start_ts)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        fields = set()
        points = []
        for row in rows:
            try:
                parsed = json.loads(row['result'])
            except Exception:
                continue

            if isinstance(parsed, dict):
                parsed = [parsed]
            if not isinstance(parsed, list):
                continue

            for item in parsed:
                if not isinstance(item, dict):
                    continue
                fields.update(item.keys())
                if filter_field and str(item.get(filter_field, '')) != filter_value:
                    continue

                if y_field not in item:
                    continue
                try:
                    points.append({'ts': row['ts'], 'value': float(item[y_field])})
                except (TypeError, ValueError):
                    continue

        return jsonify({
            'status': 'success',
            'query_id': query_id,
            'y_field': y_field,
            'filter_field': filter_field,
            'filter_value': filter_value,
            'window': window,
            'points': points,
            'fields': sorted(fields)
        })
    except Exception as e:
        print(f"[ERROR] Error building output buffer graph: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/graphs/granular', methods=['GET'])
def get_granular_graph():
    """
    Build graph points from generic SENSOR-METRICS/EVENT-METRICS output rows.
    The graph layer intentionally does not recompute aggregates.
    """
    try:
        stream = request.args.get('stream', 'sensor').strip()
        dimension_field = request.args.get('field', '').strip()
        metric = request.args.get('metric', '').strip()
        aggregate = request.args.get('aggregate', '').strip()
        field_value = request.args.get('field_value', '').strip()
        window = int(request.args.get('window', 60))

        if stream not in ('sensor', 'event'):
            return jsonify({'status': 'error', 'message': 'stream must be sensor or event'}), 400
        if not dimension_field:
            return jsonify({'status': 'error', 'message': 'field is required'}), 400
        if not metric:
            return jsonify({'status': 'error', 'message': 'metric is required'}), 400
        if not aggregate:
            return jsonify({'status': 'error', 'message': 'aggregate is required'}), 400

        query_id = 'SENSOR-METRICS' if stream == 'sensor' else 'EVENT-METRICS'
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500

        cursor = conn.cursor(dictionary=True)
        # Fetch history only; each output_buffer result already contains final aggregate values.
        cursor.execute("""
            SELECT ts, result FROM output_buffer 
            WHERE query_id = %s 
            ORDER BY ts ASC
        """, (query_id,))
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        points = []
        seen_points = set()
        available = []
        metric_aliases = {metric}
        if metric == 'speed':
            metric_aliases.add('avg_speed')
        if metric == 'avg_speed':
            metric_aliases.add('speed')
        
        for row in rows:
            try:
                data_list = json.loads(row['result'])
                if isinstance(data_list, dict):
                    data_list = [data_list]
                if not isinstance(data_list, list):
                    continue

                for item in data_list:
                    if not isinstance(item, dict):
                        continue

                    item_field = item.get('field')
                    item_field_value = item.get('field_value')
                    item_metric = item.get('metric', item.get('metric_field'))
                    item_aggregate = item.get('aggregate')
                    item_value = item.get('value')

                    if item_field is None:
                        for candidate in ('road_id', 'intersection_id', 'event_type'):
                            if candidate in item:
                                item_field = candidate
                                item_field_value = item.get(candidate)
                                break

                    if item_metric == 'avg_speed':
                        item_metric = 'speed'

                    if item_field and item_metric and item_aggregate:
                        available.append({
                            'field': item_field,
                            'field_value': item_field_value,
                            'metric': item_metric,
                            'aggregate': item_aggregate,
                        })

                    if item_field != dimension_field:
                        continue
                    if field_value and str(item_field_value).strip() != field_value:
                        continue
                    if item_metric not in metric_aliases:
                        continue
                    if item_aggregate != aggregate:
                        continue
                    if item_value is None:
                        continue
                    value = float(item_value)
                    point_key = (
                        row['ts'],
                        value,
                        item_field,
                        item_field_value,
                        item_metric,
                        item_aggregate
                    )
                    if point_key in seen_points:
                        continue
                    seen_points.add(point_key)
                    points.append({'ts': row['ts'], 'value': value})
            except Exception as e:
                print(f"[DEBUG] graph parse skipped row at ts={row.get('ts')}: {e}")
                continue

        # Slice to window
        if points:
            max_ts = points[-1]['ts']
            points = [p for p in points if p['ts'] >= (max_ts - window)]

        return jsonify({
            'status': 'success',
            'points': points,
            'available': available[:200],
            'label': f"{stream.title()} {dimension_field}={field_value or 'All'} {aggregate.upper()}({metric})"
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/queries/<query_id>', methods=['DELETE'])
def delete_query(query_id):
    """Delete a query from query_repository"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        cursor.execute("DELETE FROM query_repository WHERE query_id = %s", (query_id,))
        conn.commit()
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'status': 'error', 'message': f'Query {query_id} not found'}), 404
        
        cursor.close()
        conn.close()
        
        print(f"[DEBUG] Deleted query: {query_id}")
        return jsonify({'status': 'success', 'message': f'Query {query_id} deleted successfully'})
    except Exception as e:
        print(f"[ERROR] Error deleting query: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system/verify-reset', methods=['GET'])
def verify_reset():
    """Verify that reset was successful by showing all table counts and tick value"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        
        cursor = conn.cursor(dictionary=True)
        
        # Get counts from all stream tables
        cursor.execute("SELECT COUNT(*) as cnt FROM sensor_stream")
        sensor_count = cursor.fetchone()['cnt']
        
        cursor.execute("SELECT COUNT(*) as cnt FROM event_stream")
        event_count = cursor.fetchone()['cnt']
        
        cursor.execute("SELECT COUNT(*) as cnt FROM command_stream")
        command_count = cursor.fetchone()['cnt']
        
        cursor.execute("SELECT COUNT(*) as cnt FROM output_buffer")
        output_count = cursor.fetchone()['cnt']

        cursor.execute("SELECT COUNT(*) as cnt FROM summary_metric_values")
        summary_metric_count = cursor.fetchone()['cnt']
        
        # Get current tick
        cursor.execute("SELECT current_tick FROM system_tick WHERE id = 1")
        tick_result = cursor.fetchone()
        current_tick = tick_result['current_tick'] if tick_result else 0
        
        cursor.close()
        conn.close()
        
        print(f"[DEBUG] Verify Reset - Sensor: {sensor_count}, Event: {event_count}, Command: {command_count}, Output: {output_count}, Tick: {current_tick}")
        
        return jsonify({
            'status': 'success',
            'sensor_stream_count': sensor_count,
            'event_stream_count': event_count,
            'command_stream_count': command_count,
            'output_buffer_count': output_count,
            'summary_metric_values_count': summary_metric_count,
            'current_tick': current_tick,
            'all_cleared': sensor_count == 0 and event_count == 0 and command_count == 0 and output_count == 0 and summary_metric_count == 0 and current_tick == 0
        })
    except Exception as e:
        print(f"[ERROR] in verify_reset: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, port=5000)
