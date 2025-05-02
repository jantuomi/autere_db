from dotenv import load_dotenv
from werkzeug.utils import redirect
load_dotenv()

import os
import sys
import traceback
import tempfile
from flask import Flask, render_template, request
from flask_compress import Compress # type: ignore
from flask_apscheduler import APScheduler #type: ignore
import datetime
from dataclasses import dataclass

import uuid
from autere_db import DB, Bound, Value

app = Flask(__name__)
Compress(app)
scheduler = APScheduler()

BASE_URL = os.environ["BASE_URL"]
DB_DIR = os.environ["DB_DIR"] if "DB_DIR" in os.environ else tempfile.TemporaryDirectory().name

print("""\n"""
      f"""BASE_URL:      {BASE_URL}\n"""
      f"""DB_DIR:        {DB_DIR}\n""")

db_fields = ["id", "ts", "username", "message"]
db_types  = ["string", "int", "string", "string"]

@dataclass
class Message:
    id: str
    ts: str
    username: str
    msg: str

db = DB \
    .configure() \
    .data_dir(DB_DIR) \
    .fields(db_fields) \
    .primary_key("id") \
    .secondary_keys(["ts"]) \
    .initialize()

def error(e: str, code: int):
    error_text = f"HTTP {code}: {e}"
    return render_template("page_error.html.j2", error = error_text)

@app.errorhandler(Exception)
def error_handler(e: Exception):
    if hasattr(e, "code") and 400 >= getattr(e, "code") < 500:
        return error(str(e), getattr(e, "code"))
    else:
        traceback.print_exception(e, file=sys.stderr)
        return error("Internal Server Error", 500)

@app.get("/")
def index():
    messages = get_messages()

    htmp_target = request.form.get("htmp") or request.args.get("htmp")
    if htmp_target:
        return render_template("frag_messages.html.j2", messages = messages)
    else:
        return render_template("page_index.html.j2", messages = messages)

@app.post("/")
def query_find():
    username = request.form.get("username")
    if not username: raise ValueError("username is required")

    message = request.form.get("message")
    if not message: raise ValueError("message is required")

    id = uuid.uuid4().hex
    ts = int(datetime.datetime.now().timestamp())

    db.upsert([Value.string(id), Value.int(ts), Value.string(username), Value.string(message)])

    htmp_target = request.form.get("htmp") or request.args.get("htmp")
    if htmp_target:
        messages = get_messages()
        return render_template("frag_messages.html.j2", messages = messages)
    else:
        return redirect("/")

def get_messages():
    messages = db.range_by("ts", Bound.unbounded(), Bound.unbounded(), limit=100, sort_asc=False)

    return [
        Message(
            id=id.as_string(),
            ts=datetime.datetime.fromtimestamp(ts.as_int()).isoformat(),
            username=username.as_string(),
            msg=msg.as_string()
        )
        for [id, ts, username, msg] in messages
    ]

@scheduler.task('interval', id='my_job', minutes=1)
def run_maintenance():
    db.do_maintenance_tasks()

if __name__ == '__main__':
    scheduler.init_app(app)
    scheduler.start()
    app.run()
