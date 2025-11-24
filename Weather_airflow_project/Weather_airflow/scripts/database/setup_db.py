from database.base import create_engine_and_session
from utils.file_utils import extract_values_from_json
def setup_database(config_name, logger, echo=False):
    db_url = extract_values_from_json(r'data\config\config.json', 'db_url')
    url = db_url[config_name]
    engine, SessionLocal = create_engine_and_session(url, logger, echo=echo)
    return engine, SessionLocal

