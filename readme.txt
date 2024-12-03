Make venv
python -m venv venv

Activate venv
cmd :
venv\Scripts\activate.bat
Powershell :
venv\Scripts\Activate.ps1

Install dependencies:
pip install -r requirements.txt

Set SQLAlchemy DB connection for alembic in alembic.ini (64)


Run migrations for log_konsolidasi


API run
uvicorn app.main:apilog --reload --port 8888

SERVER run
py manage.py runserver

Django SuperUser
joshe.f
Dino1234


One more :
Needs basic_client oracle