import pandas as pd
import random
import numpy
import time
import os
from datetime import datetime

# References:
# https://simplemaps.com/data/us-cities


class DataGen:
    def __init__(self):
        self.countries = self._read_file("./data/countries.txt")
        self.first_names = self._read_file("./data/first_names.txt")
        self.last_names = self._read_file("./data/last_names.txt")
        self.cities = self._read_file("./data/cities.txt")
        self.us_cities = self._read_file("./data/us_cities.txt")
        self.us_zipcodes = self._read_file("./data/us_zipcodes.txt")
        self.us_states = self._read_file("./data/us_states.txt")
        self.boolean = ["True", "False"]
        self.boolean_integer = [1, 0]

        """df = pd.read_csv("./data/uscitiesv1.3.csv")
        df = df.sort_values(by=["state_name"], ascending=True)
        df = df["state_name"]
        df = df.unique()
        df = pd.DataFrame(df)
        df.to_csv("./data/us_cities_2.csv", index=False)"""

    def _read_file(self, filename, separator="\n"):
        file = open(filename, "r+")
        data = file.read().replace(separator, ",").split(",")
        return data

    def _pick_random_data(self, source_data, n, unique):
        max_entries = len(source_data)
        if unique and n > max_entries:
            raise Exception("n must be less than %s" % max_entries)

        col_data = list()
        for i in range(n):
            index = random.randint(0, max_entries-1)
            entry = source_data[index]
            if unique and entry in col_data:
                continue
            col_data.append(entry)

        diff = n - len(col_data)
        if diff > 0:
            diff_list = list(set(source_data).difference(col_data))
            col_data.extend(diff_list[:diff])
        return col_data

    def _generate_random_dates(self, n, unique, from_date, to_date, date_format):
        data = list()
        count = 0
        while count < n:
            stime = time.mktime(time.strptime(from_date, date_format))
            etime = time.mktime(time.strptime(to_date, date_format))

            ptime = stime + random.random() * (etime - stime)
            t = time.strftime(date_format, time.localtime(ptime))
            if unique and t in data:
                continue
            data.append(t)
            count += 1

        return data

    def generate_data(self, column_schema, output_format="CSV"):
        data = list()
        col_names = list()
        for col in column_schema:
            if col.dtype == "COUNTRIES":
                col_data = self._pick_random_data(self.countries, col.n, unique=col.unique)
            elif col.dtype == "FIRST_NAMES":
                col_data = self._pick_random_data(self.first_names, col.n, unique=col.unique)
            elif col.dtype == "LAST_NAMES":
                col_data = self._pick_random_data(self.last_names, col.n, unique=col.unique)
            elif col.dtype == "CITIES":
                col_data = self._pick_random_data(self.cities, col.n, unique=col.unique)
            elif col.dtype == "US_CITIES":
                col_data = self._pick_random_data(self.us_cities, col.n, unique=col.unique)
            elif col.dtype == "US_ZIP":
                col_data = self._pick_random_data(self.us_zipcodes, col.n, unique=col.unique)
            elif col.dtype == "US_STATES":
                col_data = self._pick_random_data(self.us_states, col.n, unique=col.unique)
            elif col.dtype == "BOOLEAN":
                col_data = self._pick_random_data(self.boolean, col.n, unique=col.unique)
            elif col.dtype == "BOOLEAN_INT":
                col_data = self._pick_random_data(self.boolean_integer, col.n, unique=col.unique)
            elif col.dtype == "DATE":
                col_data = self._generate_random_dates(col.n, unique=col.unique, from_date=col.from_date,
                                                       to_date=col.to_date, date_format=col.date_format)

            if col.sort:
                col_data = sorted(col_data)
            data.append(col_data)
            col_names.append(col.name)

        data = numpy.transpose(data)
        if output_format == "CSV":
            df = pd.DataFrame(data, columns=col_names)
            save_file = str(os.getcwd()) + "/" + str(datetime.now()) + ".csv"
            df.to_csv(save_file, index=False)
            return "CSV file saved as: %s" % save_file
        else:
            return data


class ColumnSchema:
    def __init__(self, dtype, name, n, unique=False, sort=False, from_date="1900/1/1", to_date="2100/1/1",
                 date_format="%Y/%m/%d"):
        self.dtype = dtype
        self.name = name
        self.n = n
        if dtype == "BOOLEAN" or type == "BOOLEAN_INT":
            self.unique = False
        else:
            self.unique = unique
        self.sort = sort
        self.from_date = from_date
        self.to_date = to_date
        self.date_format = date_format

schema = [ColumnSchema("FIRST_NAMES", "Name", 10, True, True),
          ColumnSchema("US_STATES", "state", 10, True),
          ColumnSchema("BOOLEAN", "bool", 10),
          ColumnSchema("DATE", "DOB", 10, True, from_date="2017/1/1", to_date="2019/1/1", sort=True)]

res = DataGen().generate_data(schema, "LIST")
print(res)