from collections import OrderedDict
from pprint import pprint

from pony.orm import *
from tqdm import tqdm

from config import *
from data import *

db = Database()

cities = {k.strip(): v for (k, v) in cities.items()}


class Schools(db.Entity):
    id = PrimaryKey(int)
    province_id = Required(int)
    city_id = Required(int)
    shahr_id = Optional(int)


class Cities(db.Entity):
    id = PrimaryKey(int)
    province_id = Required(int)
    name = Required(str)


class Provinces(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)


class Shahr(db.Entity):
    id = PrimaryKey(int)
    ostan_id = Required(int)
    shahr_type = Required(int)
    name = Required(str)


class Ostan(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)


@db_session
def correct_users(dry=False):
    schools = Schools.select()
    city_logs = dict()

    for school in tqdm(schools.where(lambda s: (s.shahr_id is None)), unit=' user', ncols=150):
        city = Cities.select().where(lambda c: (c.id == school.city_id)).first()
        province = Provinces.select().where(lambda p: (p.id == school.province_id)).first()
        suspected_city = Shahr.select().where(lambda s: (s.shahr_type == 0) and (s.name == city.name)).first()
        if suspected_city is not None:
            if dry:
                school.shahr_id = suspected_city.id
        elif str(city.name).strip() in cities.keys():
            if dry:
                school.shahr_id = cities[str(city.name).strip()]
        else:
            city_logs.setdefault(city.name, 0)
            city_logs[city.name] += 1
    cities_ordered = OrderedDict(sorted(city_logs.items(), key=lambda item: item[1], reverse=True))
    pprint(cities_ordered)
    print('distinct city names not indexed:', len(city_logs))
    print('total city names not indexed:', sum(city_logs.values()))


db.bind(**config_mariadb)
db.generate_mapping(create_tables=False)

correct_users(True)
