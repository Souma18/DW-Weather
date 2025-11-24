from etl_metadata.setup_db import *
engine_elt, SessionELT = connection_elt()
create_table(engine_elt)
