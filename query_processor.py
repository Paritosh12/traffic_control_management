import mysql.connector
import time
import json
import os
from query_builder import QueryConfigError, build_sql

with open('config.json', 'r') as f:
    config_file = json.load(f)
DB_CONFIG = config_file['database']

SOURCE_ACTIVE_NAMES = {
    'sensor_stream': {'sensor', 'sensor_stream'},
    'event_stream': {'event', 'event_stream'},
    'command_stream': {'command', 'command_stream'},
}

class QueryProcessor:
    def __init__(self, db_config):
        self.conn = mysql.connector.connect(**db_config)
        self.cursor = self.conn.cursor(dictionary=True)
        self.last_tick = None
        self.ensure_generic_summary_schema()
        print("Query Processor initialized and connected to the database.")

    def ensure_generic_summary_schema(self):
        self.cursor.execute("""
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
        self.conn.commit()

    def get_current_tick(self):
        
        self.conn.commit() # commit clears the current transaction read snapshot, allowing us to see updates made by the Input Monitor.
        self.cursor.execute("SELECT current_tick FROM system_tick WHERE id = 1")
        row = self.cursor.fetchone()
        return row['current_tick'] if row else None

    def load_query_repository(self):
        self.cursor.execute("SELECT * FROM query_repository")
        queries = self.cursor.fetchall()
        priority = {
            'SENSOR-METRICS': 10,
            'EVENT-METRICS': 20,
            'RD-SPEED-PKT': 10,
            'RD-OCC-PKT': 20,
            'RD-VEH-PKT': 30,
            'RD-EVT-PKT': 40,
            'IS-SPEED-PKT': 50,
            'IS-OCC-PKT': 60,
            'IS-VEH-PKT': 70,
            'IS-EVT-PKT': 80,
        }
        return sorted(queries, key=lambda q: (priority.get(q.get('query_id'), 100), q.get('query_id', '')))

    def is_generic_metrics_query(self, query_id):
        return query_id in {'SENSOR-METRICS', 'EVENT-METRICS'}

    def load_active_sensors(self):
        try:
            if not os.path.exists('active_sensors.json'):
                return set()
            with open('active_sensors.json', 'r') as f:
                data = json.load(f)
            return {str(item) for item in data} if isinstance(data, list) else set()
        except Exception as e:
            print(f"[DEBUG] Could not read active_sensors.json: {e}")
            return set()

    def is_query_source_active(self, query_config, active_sensors):
        source = query_config.get('source')
        names = SOURCE_ACTIVE_NAMES.get(source, {source})
        return bool(names & active_sensors)

    def latest_cumulative_results(self, query_id):
        self.cursor.execute(
            """
            SELECT result
            FROM output_buffer
            WHERE query_id = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (query_id,)
        )
        row = self.cursor.fetchone()
        if not row:
            return []
        try:
            parsed = json.loads(row['result'])
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    def row_key(self, row, group_by):
        return tuple(str(row.get(field, '')) for field in group_by)

    def numeric_value(self, value, default=0):
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def restore_number_type(self, value):
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    def coalesce_number(self, value, default=0):
        if value is None:
            return default
        return self.numeric_value(value, default)

    def positive_or_none(self, value):
        if value is None:
            return None
        numeric = self.numeric_value(value)
        return numeric if numeric >= 0 else None

    def min_or_none(self, value):
        if value is None:
            return None
        numeric = self.numeric_value(value)
        return numeric if numeric < 9999 else None

    def apply_summary_pipeline(self, query_id, query_config, tick_results, current_tick):
        if not tick_results:
            # For cumulative queries, we want to return the last known cumulative state if no new data arrived
            # so the graph remains continuous.
            print(f"[AGG][{current_tick}][{query_id}] No current tick rows; returning latest cumulative results for continuity.")
            return self.latest_cumulative_results(query_id)

        print(f"[AGG][{current_tick}][{query_id}] Merging tick results into cumulative output_buffer state.")
        return self.merge_cumulative_results(query_id, query_config, tick_results)

    def merge_cumulative_results(self, query_id, query_config, tick_results):
        group_by = query_config.get('group_by') or []
        aggregations = query_config.get('aggregations') or []

        previous_rows = self.latest_cumulative_results(query_id)
        previous_by_key = {self.row_key(row, group_by): dict(row) for row in previous_rows}
        current_by_key = {self.row_key(row, group_by): dict(row) for row in tick_results}
        merged_rows = []

        aggregation_names = {agg.get('name') for agg in aggregations if isinstance(agg, dict)}
        avg_count_aliases = {
            agg.get('name'): agg.get('count_field') or f"__count_{agg.get('name')}"
            for agg in aggregations
            if isinstance(agg, dict) and str(agg.get('operation', '')).lower() == 'avg'
        }

        for key in sorted(set(previous_by_key) | set(current_by_key)):
            prev = previous_by_key.get(key, {})
            curr = current_by_key.get(key, {})
            merged = {}

            for field in group_by:
                merged[field] = curr.get(field, prev.get(field))

            # Keep helper count columns for exact cumulative AVG calculations.
            for count_alias in avg_count_aliases.values():
                prev_count = self.numeric_value(prev.get(count_alias))
                curr_count = self.numeric_value(curr.get(count_alias))
                merged[count_alias] = self.restore_number_type(prev_count + curr_count)

            for agg in aggregations:
                name = agg.get('name')
                operation = str(agg.get('operation', '')).lower()
                if not name:
                    continue

                prev_value = self.numeric_value(prev.get(name))
                curr_value = self.numeric_value(curr.get(name))

                if operation in ('sum', 'count'):
                    value = prev_value + curr_value
                elif operation == 'max':
                    if name in prev and name in curr:
                        value = max(prev_value, curr_value)
                    else:
                        value = curr_value if name in curr else prev_value
                elif operation == 'min':
                    if name in prev and name in curr:
                        value = min(prev_value, curr_value)
                    else:
                        value = curr_value if name in curr else prev_value
                elif operation == 'avg':
                    count_alias = avg_count_aliases.get(name)
                    prev_count = self.numeric_value(prev.get(count_alias))
                    curr_count = self.numeric_value(curr.get(count_alias))
                    total_count = prev_count + curr_count
                    if total_count > 0:
                        value = ((prev_value * prev_count) + (curr_value * curr_count)) / total_count
                    else:
                        value = 0
                else:
                    value = curr.get(name, prev.get(name))

                merged[name] = self.restore_number_type(value)

            # Preserve non-aggregate fields from old SQL fallback rows if present.
            for row in (prev, curr):
                for field, value in row.items():
                    if field not in merged and field not in aggregation_names:
                        merged[field] = value

            merged_rows.append(merged)

        return merged_rows

    def should_store_cumulative_snapshot(self, query_config, results):
        return bool(results)

    def dimension_key(self, dimensions):
        return json.dumps(dimensions, sort_keys=True, separators=(',', ':'))

    def fetch_metric_summary(self, stream_name, metric_field, aggregate_name, dimension_key):
        self.cursor.execute(
            """
            SELECT stream_name, metric_field, aggregate_name, dimensions,
                   value, helper_count, last_updated_tick
            FROM summary_metric_values
            WHERE stream_name = %s AND metric_field = %s AND aggregate_name = %s AND dimensions = %s
            """,
            (stream_name, metric_field, aggregate_name, dimension_key)
        )
        return self.cursor.fetchone()

    def upsert_metric_summary(self, stream_name, metric_field, aggregate_name, dimension, value, helper_count, current_tick):
        self.cursor.execute(
            """
            INSERT INTO summary_metric_values
                (stream_name, metric_field, aggregate_name, dimensions, value, helper_count, last_updated_tick)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                helper_count = VALUES(helper_count),
                last_updated_tick = VALUES(last_updated_tick)
            """,
            (
                stream_name,
                metric_field,
                aggregate_name,
                self.dimension_key(dimension),
                value,
                helper_count,
                current_tick,
            )
        )

    def metric_display_name(self, source, field):
        if source == 'sensor_stream' and field == 'avg_speed':
            return 'speed'
        return field

    def metric_storage_field(self, source, metric):
        if source == 'sensor_stream' and metric == 'speed':
            return 'avg_speed'
        return metric

    def default_dimension_fields(self, source):
        if source in ('sensor_stream', 'event_stream'):
            return ['road_id', 'intersection_id']
        return []

    def execute_metric_tick_rows(self, source, dimension_field, metric_field, operation, current_tick):
        sql_operation = {
            'sum': 'SUM',
            'count': 'COUNT',
            'avg': 'AVG',
            'min': 'MIN',
            'max': 'MAX',
            'none': 'MAX',
        }.get(operation)
        if not sql_operation:
            raise QueryConfigError(f"Unsupported aggregate: {operation}")

        if metric_field == '*':
            metric_expr = '*'
        else:
            metric_expr = f"`{metric_field}`"

        sql = (
            f"SELECT `{dimension_field}` AS dimension_value, "
            f"{sql_operation}({metric_expr}) AS metric_value"
        )
        if operation == 'avg':
            sql += f", COUNT({metric_expr}) AS helper_count"
        sql += f" FROM `{source}` WHERE `ts` = %s GROUP BY `{dimension_field}`"

        self.cursor.execute(sql, (current_tick,))
        return self.cursor.fetchall()

    def apply_generic_metric_pipeline(self, query_id, query_config, tick_results, current_tick):
        stream_name = query_config.get('source')
        dimension_fields = query_config.get('dimension_fields') or self.default_dimension_fields(stream_name)
        aggregations = query_config.get('aggregations') or []

        final_rows = []
        for dimension_field in dimension_fields:
            for agg in aggregations:
                configured_metric = agg.get('field') or '*'
                metric_field = self.metric_storage_field(stream_name, configured_metric)
                metric_name = self.metric_display_name(stream_name, metric_field)
                operation = str(agg.get('operation') or 'count').lower()
                for row in self.execute_metric_tick_rows(stream_name, dimension_field, metric_field, operation, current_tick):
                    dimension_value = row.get('dimension_value')
                    dimension = {'field': dimension_field, 'field_value': dimension_value}
                    tick_value = self.numeric_value(row.get('metric_value'))
                    tick_count = self.numeric_value(row.get('helper_count'), 0)
                    dim_key = self.dimension_key(dimension)
                    previous = self.fetch_metric_summary(stream_name, metric_name, operation, dim_key)
                    prev_value = self.numeric_value(previous.get('value') if previous else None)
                    prev_count = self.numeric_value(previous.get('helper_count') if previous else None)

                    if operation in ('sum', 'count'):
                        final_value = prev_value + tick_value
                        final_count = prev_count + tick_count if tick_count else prev_count
                    elif operation == 'avg':
                        final_count = prev_count + tick_count
                        final_value = ((prev_value * prev_count) + (tick_value * tick_count)) / final_count if final_count else 0
                    elif operation == 'max':
                        final_value = max(prev_value, tick_value) if previous else tick_value
                        final_count = prev_count
                    elif operation == 'min':
                        final_value = min(prev_value, tick_value) if previous else tick_value
                        final_count = prev_count
                    elif operation == 'none':
                        final_value = tick_value
                        final_count = 1
                    else:
                        final_value = tick_value
                        final_count = prev_count

                    self.upsert_metric_summary(
                        stream_name,
                        metric_name,
                        operation,
                        dimension,
                        final_value,
                        final_count,
                        current_tick,
                    )
                    final_rows.append({
                        'field': dimension_field,
                        'metric': metric_name,
                        'aggregate': operation,
                        'field_value': dimension_value,
                        'value': self.restore_number_type(final_value),
                    })

        if final_rows:
            return final_rows

        self.cursor.execute(
            """
            SELECT stream_name, metric_field, aggregate_name, dimensions, value
            FROM summary_metric_values
            WHERE stream_name = %s
            ORDER BY metric_field, aggregate_name, dimensions
            """,
            (stream_name,)
        )
        for item in self.cursor.fetchall():
            try:
                dimension = json.loads(item.get('dimensions') or '{}')
            except Exception:
                dimension = {}
            final_row = {
                'field': dimension.get('field'),
                'metric': item['metric_field'],
                'aggregate': item['aggregate_name'],
                'field_value': dimension.get('field_value'),
                'value': self.restore_number_type(self.numeric_value(item['value'])),
            }
            final_rows.append(final_row)
        return final_rows

    def process_queries(self, current_tick):
        queries = self.load_query_repository()
        active_sensors = self.load_active_sensors()
        
        for q in queries:
            qid = q['query_id']
            freq = q['frequency_sec']
            last_run = q['last_run']

            # Tick represents the "stream time" not real time!
            # Run if: last_run is 0 (first time) OR enough ticks have passed
            should_run = False
            
            if last_run == 0:
                # First run - execute immediately
                should_run = True
            elif (current_tick - last_run) >= freq:
                # Enough ticks have passed
                should_run = True
            
            if should_run:
                print(f"[DEBUG] [{current_tick}] Executing {qid}: {q['description']}")
                
                try:
                    if q.get('query_config'):
                        query_config = q['query_config']
                        if isinstance(query_config, str):
                            query_config = json.loads(query_config)
                        if not self.is_query_source_active(query_config, active_sensors):
                            print(f"[DEBUG]       -> Skipping {qid}; source {query_config.get('source')} is not active.")
                            continue
                        if self.is_generic_metrics_query(qid):
                            results = self.apply_generic_metric_pipeline(qid, query_config, [], current_tick)
                        else:
                            sql_to_run, params = build_sql(query_config, current_tick)
                            self.cursor.execute(sql_to_run, params)
                            tick_results = self.cursor.fetchall()
                            print(f"[AGG][{current_tick}][{qid}] Current tick raw aggregate rows: {tick_results}")
                            results = self.apply_summary_pipeline(qid, query_config, tick_results, current_tick)
                    elif q.get('query_sql'):
                        # Temporary compatibility with repositories created before query_config.
                        sql_to_run = q['query_sql'].replace('{CURRENT_TICK}', str(current_tick))
                        self.cursor.execute(sql_to_run)
                        results = self.cursor.fetchall()
                    else:
                        raise QueryConfigError("query_config is missing")

                    if q.get('query_config') and self.should_store_cumulative_snapshot(query_config, results):
                        res_json = json.dumps(results, default=str)
                        self.cursor.execute(
                            "INSERT INTO output_buffer (query_id, ts, result) VALUES (%s, %s, %s)",
                            (qid, current_tick, res_json)
                        )
                        print(f"[AGG][{current_tick}][{qid}] Inserted graph-ready final aggregate rows into output_buffer: {results}")
                    elif results:
                        res_json = json.dumps(results, default=str)
                        self.cursor.execute(
                            "INSERT INTO output_buffer (query_id, ts, result) VALUES (%s, %s, %s)",
                            (qid, current_tick, res_json)
                        )
                        print(f"[DEBUG]       -> Stored {len(results)} results in output_buffer.")
                    else:
                        print(f"[DEBUG]       -> No results for this query.")
                    
                    
                    self.cursor.execute(
                        "UPDATE query_repository SET last_run = %s WHERE query_id = %s",
                        (current_tick, qid)
                    )
                    self.conn.commit()

                except Exception as e:
                    print(f"[ERROR] executing {qid}: {e}")

    def listen(self, poll_interval=1):
        print("[DEBUG] Query Processor: Listening for tick changes...")
        
        while True:
            try:
                current_tick = self.get_current_tick()
                if current_tick and current_tick != self.last_tick:
                    print(f"\n[DEBUG] >>> Tick Advanced to {current_tick} <<<")
                    self.process_queries(current_tick)
                    self.last_tick = current_tick
                
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                print("\n[DEBUG] Query Processor stopped by user.")
                break
            except Exception as e:
                print(f"[ERROR] Processor loop error: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(5) 

if __name__ == "__main__":
    processor = QueryProcessor(DB_CONFIG)
    processor.listen()
