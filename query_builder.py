import re


class QueryConfigError(ValueError):
    pass


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SOURCE_FIELDS = {
    "sensor_stream": {"pkt_id", "intersection_id", "road_id", "signal_id", "vehicle_count", "avg_speed", "occupancy", "ts"},
    "event_stream": {"event_id", "event_type", "intersection_id", "road_id", "priority", "ts"},
    "command_stream": {"cmd_id", "signal_id", "action", "duration", "reason", "ts"},
}

ALLOWED_OPERATIONS = {
    "sum": "SUM",
    "count": "COUNT",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "none": "MAX",
}

ALLOWED_OPERATORS = {
    "=",
    "!=",
    "<>",
    ">",
    ">=",
    "<",
    "<=",
}


def _require_identifier(value, label):
    if value == "*" and label == "aggregation field":
        return value
    if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
        raise QueryConfigError(f"Invalid {label}: {value!r}")
    return value


def _quote_identifier(value):
    _require_identifier(value, "identifier")
    return f"`{value}`"


def _require_source_field(source, field, label):
    _require_identifier(field, label)
    if field not in SOURCE_FIELDS[source]:
        raise QueryConfigError(f"Unknown {label} for {source}: {field!r}")
    return field


def _build_filter_clause(source, filter_config, current_tick):
    if not filter_config:
        return "", []

    filters = filter_config if isinstance(filter_config, list) else [filter_config]
    clauses = []
    params = []

    for item in filters:
        if not isinstance(item, dict):
            raise QueryConfigError("Each filter must be an object")

        field = _require_source_field(source, item.get("field"), "filter field")
        operator = str(item.get("operator", "=")).strip()
        if operator not in ALLOWED_OPERATORS:
            raise QueryConfigError(f"Unsupported filter operator: {operator}")

        value = item.get("value")
        if value == "{CURRENT_TICK}":
            value = current_tick

        clauses.append(f"{_quote_identifier(field)} {operator} %s")
        params.append(value)

    return " WHERE " + " AND ".join(clauses), params


def build_sql(query_config, current_tick):
    """
    Convert a structured SDMS query config into parameterized MySQL SQL.

    Returns:
        (sql, params)
    """
    if not isinstance(query_config, dict):
        raise QueryConfigError("Query config must be an object")

    source = query_config.get("source")
    if source not in SOURCE_FIELDS:
        raise QueryConfigError(f"Unsupported query source: {source}")

    group_by = query_config.get("group_by") or []
    if not isinstance(group_by, list):
        raise QueryConfigError("group_by must be a list")

    aggregations = query_config.get("aggregations") or []
    if not isinstance(aggregations, list) or not aggregations:
        raise QueryConfigError("aggregations must contain at least one item")

    select_parts = []
    for field in group_by:
        select_parts.append(_quote_identifier(_require_source_field(source, field, "group_by field")))

    for agg in aggregations:
        if not isinstance(agg, dict):
            raise QueryConfigError("Each aggregation must be an object")

        alias = _require_identifier(agg.get("name"), "aggregation name")
        operation = str(agg.get("operation", "")).lower()
        if operation not in ALLOWED_OPERATIONS:
            raise QueryConfigError(f"Unsupported aggregation operation: {operation}")

        field = agg.get("field", "*")
        if operation == "none" and field == "*":
            raise QueryConfigError("none operation requires a concrete field")
        if operation == "count" and field == "*":
            field_sql = "*"
        else:
            field_sql = _quote_identifier(_require_source_field(source, field, "aggregation field"))

        select_parts.append(f"{ALLOWED_OPERATIONS[operation]}({field_sql}) AS {_quote_identifier(alias)}")
        if operation == "avg":
            select_parts.append(f"COUNT({field_sql}) AS {_quote_identifier('__count_' + alias)}")

    where_clause, params = _build_filter_clause(source, query_config.get("filter"), current_tick)

    sql = f"SELECT {', '.join(select_parts)} FROM {_quote_identifier(source)}{where_clause}"

    
    if group_by:
        sql += " GROUP BY " + ", ".join(_quote_identifier(field) for field in group_by)

    return sql, params
