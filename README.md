## Required libraries
* Alembic 1.16.1 or higher
* Alive progress 3.2.0 or higher
* Great Expectations 1.4.4 or higher
* Pandas 2.2.3 or higher
* Psycopg2 2.9.10 or higher
* SQLAlchemy 2.0.41 or higher

## Other requirements
* PostgreSQL server

# Onboarding
1. in db.py replace the string in DATABASE_URL with your postgreSQL url
2. do the same for sqlalchemy.url inside alembic.ini

## Using program with Alive progress
* if your using pycharm or similar program make sure emulate terminal in output console is turned on

## Using program without Alive progress
1. comment out or delete "from alive_progress import alive_bar" from app.py
2. comment out or delete "with alive_bar(len(transaction_df)) as bar:" from app.y (don't forget to decrease the increment by 1)
3. comment out or delete any instance of "bar()" in app.py
4. remove the comment signs from "def progress_bar(current, percent):" function
5.  remove the comment signs from any instance of "#progress_bar(index, 1000)"