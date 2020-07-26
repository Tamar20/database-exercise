import csv
from dataclasses import dataclass
from typing import List, Dict, Any

from dataclasses_json import dataclass_json

from db_api import DBField, DBTable, SelectionCriteria


@dataclass_json
@dataclass
class DataBase:
    def __init__(self):  # TODO: write to file
        self.num_of_tables = 0  # TODO: is necessary?
        self.tables_names = {}

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        if table_name in self.tables_names.keys():
            raise FileExistsError("This table already exists")
        with open(f"{table_name}.csv", "w") as table_file:
            table = csv.writer(table_file)
            table.writerow(fields)

        self.tables_names[table_name] = [fields, key_field_name]

        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        raise NotImplementedError

    def get_table(self, table_name: str) -> DBTable:
        raise NotImplementedError

    def delete_table(self, table_name: str) -> None:
        raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
        raise NotImplementedError

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
