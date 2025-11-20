from database import engine_elt, engine_clean, engine_transform, BaseELT, BaseClean, BaseTransform
import models
import clean_models
import log

BaseELT.metadata.create_all(bind=engine_elt)
BaseClean.metadata.create_all(bind=engine_clean)
BaseTransform.metadata.create_all(bind=engine_transform)