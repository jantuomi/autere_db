from dotenv import load_dotenv
load_dotenv()

import os
import sys
import traceback
import tempfile
from flask import Flask, render_template
from flask_compress import Compress # type: ignore

from log_db import DB, Bound

app = Flask(__name__)
Compress(app)

BASE_URL = os.environ["BASE_URL"]
DB_DIR = os.environ["DB_DIR"] if "DB_DIR" in os.environ else tempfile.TemporaryDirectory().name

print("""\n"""
      f"""BASE_URL:      {BASE_URL}\n"""
      f"""DB_DIR:        {DB_DIR}\n""")

db = DB \
    .configure() \
    .data_dir(DB_DIR) \
    .fields(["id", "name"]) \
    .primary_key("id") \
    .secondary_keys(["name"]) \
    .initialize()

def error_page(message: str, code: int = 400):
    return render_template("error.html.j2", error = message), code

@app.errorhandler(404)
def not_found(e: Exception):
    return error_page("Not found", 404)

@app.errorhandler(405)
def method_not_allowed(e: Exception):
    return error_page(str(e), 405)

@app.errorhandler(Exception)
def error_handler(e: Exception):
    traceback.print_exception(e, file=sys.stderr)
    return error_page("Internal server error", 500)

@app.get("/")
def index():
    return page_find()

@app.get("/find")
def page_find():
    rows = db.range_by("id", Bound.unbounded(), Bound.unbounded(), limit=100)

    return render_template('index.html.j2',
        selected_form = "form_find.html.j2",
        field_names = ["id", "name"],
        rows = rows,
    )

@app.get("/range")
def page_range():
    rows = db.range_by("id", Bound.unbounded(), Bound.unbounded(), limit=100)

    return render_template('index.html.j2',
        selected_form = "form_range.html.j2",
        field_names = ["id", "name"],
        rows = rows,
    )
