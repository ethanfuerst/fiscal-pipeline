from sqlmesh import macro


@macro()
def try_cast_to_float(evaluator, column_name: str):
    return f'coalesce(try_cast({column_name} as decimal), 0)'


@macro()
def try_strip_date(evaluator, column_name: str):
    return f"strptime(if({column_name} = '', null, {column_name}), '%m/%d/%Y')"
