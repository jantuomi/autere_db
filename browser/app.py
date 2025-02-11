from dotenv import load_dotenv
load_dotenv()

import os
import sys
import traceback
import tempfile
from flask import Flask, render_template, request
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

@app.errorhandler(Exception)
def error_handler(e: Exception):
    if e.code >= 500: # type: ignore
        traceback.print_exception(e, file=sys.stderr)
        error_text = "Internal Server Error"
    else:
        error_text = str(e)

    htmz_target = request.form.get("htmz")
    if htmz_target:
        return render_template("frag_error.html.j2", error = error_text, container = htmz_target)
    else:
        return render_template("page_error.html.j2", error = error_text)

@app.get("/")
def index():
    return page_find()

@app.get("/find")
def page_find():
    rows = db.range_by("id", Bound.unbounded(), Bound.unbounded(), limit=100)

    return render_template('page_main.html.j2',
        selected_form = "frag_form_find.html.j2",
        field_names = ["id", "name"],
        rows = rows,
    )

@app.get("/range")
def page_range():
    rows = db.range_by("id", Bound.unbounded(), Bound.unbounded(), limit=100)

    return render_template('page_main.html.j2',
        selected_form = "frag_form_range.html.j2",
        field_names = ["id", "name"],
        rows = rows,
    )
