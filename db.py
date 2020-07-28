import csv
import json
import os
from dataclasses import dataclass
from typing import List, Dict, Any

from dataclasses_json import dataclass_json

import db_api
from db_api import DBField, DBTable, SelectionCriteria, DB_ROOT


# ------------------------------------- DBTable ------------------------------------- #
@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def count(self) -> int:  # TODO: insert to a variable
        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data = csv.reader(table_file)
            # print(len(table_data))
            # return len(table_data)
            counter = 0
            for row in table_data:
                if row:
                    counter += 1
                    print(counter)
            print("count:", counter - 1)
        return counter - 1

    def insert_record(self, values: Dict[str, Any]) -> None:
        print("=== insert_record ===")
        with open(DB_ROOT / f"{self.name}.csv", "r+") as table_file:
            table_data = csv.reader(table_file)

            for row in table_data:
                fields = row
                break

            new_row = []
            for f in fields:
                new_row += [values[f]]
            print("new_row:", new_row)
            table_data = csv.writer(table_file)
            table_data.writerow(new_row)

    def delete_record(self, key: Any) -> None:
        print("=== delete_record ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

        key_field = db_data[self.name]["key"]
        key_index = [list(d.keys())[0] for d in db_data[self.name]["fields"]].index(key_field)

        with open(DB_ROOT / f"{self.name}.csv", "r+") as table_file:
            table_data = csv.reader(table_file)
            new_table_data = []

            for row in table_data:
                if row and str(row[key_index]) != str(key):
                    new_table_data.append(row)
                    print("row: ", row)
        with open(DB_ROOT / f"{self.name}.csv", "w") as table_file:
            table_data = csv.writer(table_file)
            table_data.writerows(new_table_data)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        print("=== get_record ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

        key_field = db_data[self.name]["key"]
        key_index = [d.key() for d in db_data[self.name]["fields"]].index(key_field)

        with open(DB_ROOT / f"{self.name}.csv", "r+") as table_file:
            table_data = csv.writer(table_file)

        for row in table_data:
            if row[key_index] == key:
                table_data.writerow(row)

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        print("=== update_record ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

        key_field = db_data[self.name]["key"]
        fileds_list = db_data[self.name]["fields"]
        key_index = [list(d.keys())[0] for d in fileds_list].index(key_field)

        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data = csv.reader(table_file)
            new_table_data = []
        print("table_data: ", table_data)
        for row in table_data:
            if row and row[key_index] != key:
                new_table_data.append(row)

        new_row = [values[f] for f in fileds_list]
        new_table_data.append(new_row)
        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data.writerows(new_table_data)

    def get_query_res(self, criterion: SelectionCriteria):
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

        fields_list = db_data[self.name]["fields"]
        field_index = [d.key() for d in fields_list].index(criterion.field_name)

        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data = csv.reader(table_file)
            res = set()

        for row in table_data:
            if f"{row[field_index]} {criterion.operator} {criterion.value}":
                res.add(row)
        return res

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        print("=== query_table ===")
        res = set()

        for SCriteria in criteria:
            res.add(self.get_query_res(SCriteria))
        dict_res = {}

        for i, row in enumerate(res, 1):
            dict_res[i] = row
        return dict_res

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


# ------------------------------------- DataBase ------------------------------------ #
@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    # def __init__(self):
    #     print("=== init ===")
    #     if os.path.isfile(DB_ROOT / "DataBase.json") and os.access('./db_files/DataBase.json', os.R_OK):
    #         print("File exists and is readable/there")
    #         with (DB_ROOT / "DataBase.json").open("r") as DB_file:
    #             print(json.load(DB_file))
    #     else:
    #         with (DB_ROOT / "DataBase.json").open("w") as DB_file:
    #             json.dump({}, DB_file)
    def __init__(self):  # TODO:
        print("=== init ===")
        if os.path.isfile(DB_ROOT / "DataBase.json") and os.access('./db_files/DataBase.json', os.R_OK):
            print("File exists and is readable/there")
        else:
            with open(DB_ROOT / "DataBase.json", "w") as DB_file:
                json.dump({}, DB_file)

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

            if table_name in db_data.keys():
                raise FileExistsError("This table already exists")

        with open(DB_ROOT / f"{table_name}.csv", "w") as table_file:
            table = csv.writer(table_file)
            table.writerow([f.name for f in fields])
            db_data[table_name] = {
                "fields": [{f.name: str(f.type)} for f in fields],
                "key": key_field_name
            }

        with open(DB_ROOT / "DataBase.json", "w") as DB_file:
            json.dump(db_data, DB_file)

        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)
        # print(len(db_data), db_data)
        print("db_data: ", db_data)
        print("len(db_data): ", len(db_data))
        return len(db_data)

    def get_table(self, table_name: str) -> DBTable:
        print("=== get_table ===")
        with open(DB_ROOT / "DataBase.json", "r+") as DB_file:
            db_data = json.load(DB_file)
            # fields_list = [DBField(f.key(), f[f.key()]) for f in db_data[table_name]["fields"]]
            fields_list = []
            for dict_ in db_data[table_name]["fields"]:
                print(dict_)
                fields_list.append(dict_)

        return DBTable(table_name, fields_list, db_data[table_name]["key"])

    def delete_table(self, table_name: str) -> None:
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)
            if table_name not in db_data.keys():
                raise FileExistsError("This table does not exist")
            # del db_data[table_name]
            new_dict = {}
            for tb_name in db_data.keys():
                if tb_name != table_name:
                    new_dict[tb_name] = db_data[tb_name]
                else:
                    os.remove(DB_ROOT / f"{tb_name}.csv")

        with open(DB_ROOT / "DataBase.json", "w") as DB_file:
            json.dump(new_dict, DB_file)
        # os.remove(f"{table_name}.csv")

    def get_tables_names(self) -> List[Any]:
        with open(DB_ROOT / "DataBase.json", "r+") as DB_file:
            db_data = json.load(DB_file)
            res = []
            [res.append(k) for k in db_data.keys()]
            print(res)
        return res

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
