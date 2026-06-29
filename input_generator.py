import os
import time
import random

# Note: InputGenerator only generates and dumps XML files
# InputMonitor will read and process them independently

# Domain values for randomization
INTERSECTIONS = ["I101"]
ROADS = ["I101_R1", "I101_R2", "I101_R3", "I101_R4"]
EVENT_TYPES = ["Emergency", "Pedestrian", "VIP_Convoy", "Accident"]
ACTIONS = ["GREEN", "RED", "YELLOW"]
REASONS = ["NORMAL", "EMERGENCY", "MAINTENANCE"]

def generate_sensor_xml(file_path, base_ts, start_id):
    """Generates a random sensor stream XML and returns the next ID."""
    curr_id = start_id
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<SensorStreamInput>\n'
    
    # Generate random number of sensor events per burst
    num_events = random.randint(1, 8)
    for _ in range(num_events):
        road = random.choice(ROADS)
        signal = f"{road}_TL"
        event_ts = base_ts
        xml_content += f"""    <sensor_event>
        <pkt_id>PKT_{curr_id:06d}</pkt_id>
        <intersection_id>{INTERSECTIONS[0]}</intersection_id>
        <road_id>{road}</road_id>
        <signal_id>{signal}</signal_id>
        <vehicle_count>{random.randint(5, 50)}</vehicle_count>
        <avg_speed>{round(random.uniform(10.0, 60.0), 1)}</avg_speed>
        <occupancy>{round(random.uniform(10.0, 95.0), 1)}</occupancy>
        <ts>{event_ts}</ts>
    </sensor_event>\n"""
        curr_id += 1
        
    xml_content += "</SensorStreamInput>"
    
    with open(file_path, "w") as f:
        f.write(xml_content)
    return curr_id

def generate_event_xml(file_path, base_ts, start_id):
    """Generates a random event stream XML and returns the next ID."""
    curr_id = start_id
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<EventStreamInput>\n'
    
    # Generate random number of event stream events per burst
    num_events = random.randint(0, 3)
    for _ in range(num_events):
        road = random.choice(ROADS)
        event_ts = base_ts
        xml_content += f"""    <event>
        <event_id>EVT_{curr_id:06d}</event_id>
        <event_type>{random.choice(EVENT_TYPES)}</event_type>
        <intersection_id>{INTERSECTIONS[0]}</intersection_id>
        <road_id>{road}</road_id>
        <priority>{random.randint(1, 3)}</priority>
        <ts>{event_ts}</ts>
    </event>\n"""
        curr_id += 1
        
    xml_content += "</EventStreamInput>"
    
    with open(file_path, "w") as f:
        f.write(xml_content)
    return curr_id

def generate_command_xml(file_path, base_ts, start_id):
    """Generates a random command stream XML and returns the next ID."""
    curr_id = start_id
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<CommandStreamInput>\n'
    
    # Generate 0 to 2 command elements per burst
    num_events = random.randint(0, 2)
    for _ in range(num_events):
        road = random.choice(ROADS)
        signal = f"{road}_TL"
        event_ts = base_ts
        xml_content += f"""    <command>
        <cmd_id>CMD_{curr_id:06d}</cmd_id>
        <signal_id>{signal}</signal_id>
        <action>{random.choice(ACTIONS)}</action>
        <duration>{random.randint(10, 60)}</duration>
        <reason>{random.choice(REASONS)}</reason>
        <ts>{event_ts}</ts>
    </command>\n"""
        curr_id += 1
        
    xml_content += "</CommandStreamInput>"
    
    with open(file_path, "w") as f:
        f.write(xml_content)
    return curr_id


if __name__ == "__main__":
    base_path = '.'
    data_dir = os.path.join(base_path, 'data')
    
    sensor_file = os.path.join(data_dir, 'dummy_sensor.xml')
    event_file = os.path.join(data_dir, 'dummy_event.xml')
    command_file = os.path.join(data_dir, 'dummy_command.xml')

    os.makedirs(data_dir, exist_ok=True)
    
    print("[InputGenerator] Starting InputGenerator")
    print("[InputGenerator] Will dump XML files to: " + data_dir)

    ts = 0        
    pkt_id = 1    
    evt_id = 1
    cmd_id = 1

    print("[InputGenerator] Starting Continuous Stream Input Generator...")
    print("[InputGenerator] Stream Logical Time starts at ts=0")
    print("[InputGenerator] Press Ctrl+C to stop generation.\n")
    
    try:
        import json
        while True:
            enabled_sensors = []
            if os.path.exists('active_sensors.json'):
                try:
                    with open('active_sensors.json', 'r') as f:
                        enabled_sensors = json.load(f)
                except Exception as e:
                    print(f"[InputGenerator] Error reading active_sensors.json: {e}")
                    pass

            print(f"[InputGenerator] Enabled sensors: {enabled_sensors}")

            
            if "sensor" in enabled_sensors or "sensor_stream" in enabled_sensors:
                pkt_id = generate_sensor_xml(sensor_file, ts, pkt_id)
                print(f"[InputGenerator] Generated sensor XML (ts={ts}, next_id={pkt_id}) -> {sensor_file}")
                
            if "event" in enabled_sensors or "event_stream" in enabled_sensors:
                evt_id = generate_event_xml(event_file, ts, evt_id)
                print(f"[InputGenerator] Generated event XML (ts={ts}, next_id={evt_id}) -> {event_file}")
                
            if "command" in enabled_sensors or "command_stream" in enabled_sensors:
                cmd_id = generate_command_xml(command_file, ts, cmd_id)
                print(f"[InputGenerator] Generated command XML (ts={ts}, next_id={cmd_id}) -> {command_file}")

            # advance the tick by 1 unit and pause execution for 5 real world seconds
            ts += 1
            
            print(f"[InputGenerator] [ts={ts}] (Waiting 5 seconds before next burst...)\n")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("[InputGenerator] Input generation stopped by user.")
