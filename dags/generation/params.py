from typing import Optional

from airflow.models import Param

from generation.fields import ParamFields

import datetime


def generate_params(source: Optional[dict]):
    if source is None:
        return None
    else:
        params = {}
        for key, value in source.items():
            if isinstance(value, str):
                params[key] = eval(value)
            elif isinstance(value, dict) and ParamFields.default in value:
                params[key] = Param(
                    default=value.get(ParamFields.default),
                    **ParamFields.filter_dict(value)

                )
            else:
                params[key] = value

        return params
