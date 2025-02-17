from dotenv import load_dotenv
load_dotenv()

import os
import sys
import traceback
import tempfile
from typing import cast
from flask import Flask, render_template, request
from flask_compress import Compress # type: ignore

import log_db
from log_db import DB, Bound, Value

app = Flask(__name__)
Compress(app)

BASE_URL = os.environ["BASE_URL"]
DB_DIR = os.environ["DB_DIR"] if "DB_DIR" in os.environ else tempfile.TemporaryDirectory().name

print("""\n"""
      f"""BASE_URL:      {BASE_URL}\n"""
      f"""DB_DIR:        {DB_DIR}\n""")

db_fields = ["id", "name"]
db_types = ["int", "string"]

db = DB \
    .configure() \
    .data_dir(DB_DIR) \
    .fields(db_fields) \
    .primary_key("id") \
    .secondary_keys(["name"]) \
    .initialize()

def error(e: str, code: int):
    error_text = f"HTTP {code}: {e}"
    #return render_template("page_error.html.j2", error = error_text)

    htmp_target = request.form.get("htmp") or request.args.get("htmp")
    if htmp_target:
        return render_template("frag_error.html.j2", error = error_text, container = htmp_target)
    else:
        return render_template("page_error.html.j2", error = error_text)

@app.errorhandler(Exception)
def error_handler(e: Exception):
    if hasattr(e, "code") and 400 >= getattr(e, "code") < 500:
        return error(str(e), getattr(e, "code"))
    else:
        traceback.print_exception(e, file=sys.stderr)
        return error("Internal Server Error", 500)

@app.get("/")
def index_default():
    return index("find")

@app.get("/<op>")
def index(op: str):
    if op not in ["find", "range", "upsert", "delete"]:
        return error("Invalid operation", 400)

    rows = db.range_by("id", Bound.unbounded(), Bound.unbounded(), limit=100)
    rows = [[value_to_str(v) for v in row] for row in rows]

    htmp_target = request.form.get("htmp") or request.args.get("htmp")
    if htmp_target:
        return render_template(f"frag_form_{op}.html.j2")
    else:
        return render_template('page_main.html.j2',
            selected_form = f"frag_form_{op}.html.j2",
            field_names = ["id", "name"],
            rows = rows,
        )

@app.post("/find")
def query_find():
    field = request.form.get("field")
    if not field: raise ValueError("Field is required")

    values = request.form.get("values")
    if not values: raise ValueError("Values are required")

    field_index = db_fields.index(field)
    if field_index == -1: raise ValueError(f"Field '{field}' not found")

    try:
        values = [cast_to_value(field, v) for v in values.split("\n")]
        values = [v for v in values if v is not None]
    except ValueError as e:
        return error(str(e), 400)

    tagged_rows = db.batch_find_by(field, values, limit=100)
    rows = [[value_to_str(v) for v in row] for (_, row) in tagged_rows]

    htmp_target = request.form.get("htmp") or request.args.get("htmp")
    if htmp_target:
        return render_template("frag_results.html.j2",
            field_names = ["id", "name"],
            rows = rows,
        )
    else:
        return render_template('page_main.html.j2',
            selected_form = "frag_form_find.html.j2",
            field_names = ["id", "name"],
            rows = rows,
        )

@app.post("/range")
def query_range():
    field = request.form.get("field")
    if not field: raise ValueError("Field is required")

    from_type = request.form.get("from_type")
    if not from_type: raise ValueError("From type is required")

    to_type = request.form.get("to_type")
    if not to_type: raise ValueError("To type is required")

    field_index = db_fields.index(field)
    if field_index == -1: raise ValueError(f"Field '{field}' not found")

    match from_type:
        case "unbounded":
            bound_lower = Bound.unbounded()
        case "included":
            from_value = request.form.get("from_value")
            try:
                if not from_value: raise ValueError("From value is required")
                from_value = cast_to_value(field, from_value)
            except ValueError as e:
                return error(str(e), 400)
            bound_lower = Bound.included(cast(Value, from_value))
        case "excluded":
            from_value = request.form.get("from_value")
            try:
                if not from_value: raise ValueError("From value is required")
                from_value = cast_to_value(field, from_value)
            except ValueError as e:
                return error(str(e), 400)
            bound_lower = Bound.excluded(cast(Value, from_value))
        case _:
            return error("Invalid from type", 400)

    match to_type:
        case "unbounded":
            bound_upper = Bound.unbounded()
        case "included":
            to_value = request.form.get("to_value")
            try:
                if not to_value: raise ValueError("To value is required")
                to_value = cast_to_value(field, to_value)
            except ValueError as e:
                return error(str(e), 400)
            bound_upper = Bound.included(cast(Value, to_value))
        case "excluded":
            to_value = request.form.get("to_value")
            try:
                if not to_value: raise ValueError("To value is required")
                to_value = cast_to_value(field, to_value)
            except ValueError as e:
                return error(str(e), 400)
            bound_upper = Bound.excluded(cast(Value, to_value))
        case _:
            return error("Invalid to type", 400)

    tagged_rows = db.range_by(field, bound_lower, bound_upper, limit=100)
    rows = [[value_to_str(v) for v in row] for row in tagged_rows]

    htmp_target = request.form.get("htmp") or request.args.get("htmp")
    if htmp_target:
        return render_template("frag_results.html.j2",
            field_names = ["id", "name"],
            rows = rows,
        )
    else:
        return render_template('page_main.html.j2',
            selected_form = "frag_form_range.html.j2",
            field_names = ["id", "name"],
            rows = rows,
        )

# Utils

def cast_to_value(field: str, str_value: str) -> Value | None:
    str_value = str_value.strip()
    if str_value == "": return None
    if str_value[0] == "\"":
        if str_value[-1] != "\"": raise ValueError(f"Invalid string: {str_value}")
        str_value = str_value[1:-1]

    field_index = db_fields.index(field)
    if field_index == -1: raise ValueError(f"Field '{field}' not found")

    type = db_types[field_index]

    match type:
        case "int":    return Value.int(int(str_value))
        case "string": return Value.string(str_value)
        case _:        raise ValueError(f"Unsupported type: {type}")

def value_to_str(value: Value) -> str:
    match value.kind():
        case log_db.VALUE_INT:     return f"{str(value.as_int())} (int)"
        case log_db.VALUE_STRING:  return f"\"{value.as_string()}\" (string)"
        case log_db.VALUE_DECIMAL: return f"{value.as_decimal()} (decimal)"
        case log_db.VALUE_BYTES:   return f"{value.as_bytes()} (bytes)"
        case log_db.VALUE_NULL:    return "null"
        case _:                    raise ValueError(f"Unsupported value kind: {value.kind()}")
