# Demo chat app

This is a demo chat app built using AutereDB, Python and Flask.

## Installation

Get a Python 3 environment running on your machine (pref. with virtualenv). Then:

```bash
pip install -r requirements.txt
DB_DIR=${PWD}/db_data BASE_URL=localhost:8000 python -m flask --app app --debug run -p 8000
```
