from enum import Enum


class FieldsMixin(Enum):
    @classmethod
    def filter_dict(cls, source):
        enum_values_set = {field.value for field in cls}
        return {k: v for k, v in source.items() if k not in enum_values_set}

    def __str__(self):
        return self.value


class DagFields(str, FieldsMixin):
    id = "id"
    graph = "graph"
    start_date = "start_date"
    params = "params"
    extra = "extra"


class GraphFields(str, FieldsMixin):
    id = "id"
    type = "type"
    items = "items"
    operator = "operator"
    extra = "extra"


class ParamFields(str, FieldsMixin):
    default = "default"
