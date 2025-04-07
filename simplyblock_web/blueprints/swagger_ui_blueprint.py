import os

import yaml
import json
from flask_swagger_ui import get_swaggerui_blueprint

SWAGGER_URL="/swagger"

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
API_URL=f"{SCRIPT_PATH}/../static/swagger.yaml"

cnf = {}
with open(API_URL) as f:
    cnf = f.read()
    cnf = yaml.load(cnf, Loader=yaml.SafeLoader)

# # Convert non-serializable types to strings
cnf = json.loads(json.dumps(cnf, default=str))

bp = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': 'SimplyBlock-API',
        'spec': cnf,
    }
)
