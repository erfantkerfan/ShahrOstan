from collections import OrderedDict
from pprint import pprint

from pony.orm import *
from tqdm import tqdm

from data import *

config = {
    'provider': 'mysql',
    'user': 'root',
    'password': 'root',
    'host': '127.0.0.1',
    'database': 'alaatv',
}
db = Database()

cities = {k.strip(): v for (k, v) in cities.items()}


class Users(db.Entity):
    id = PrimaryKey(int)
    firstName = Optional(str)
    lastName = Optional(str)
    mobile = Required(str)
    nationalCode = Required(str)
    province = Optional(str)
    shahr_id = Optional(int)
    city = Optional(str, nullable=True)
    deleted_at = Optional(str)


class Shahr(db.Entity):
    id = PrimaryKey(int)
    ostan_id = Required(int)
    shahr_type = Required(int)
    name = Required(str)


class Ostan(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)


@db_session
def run(dry=False):
    users = Users.select()
    city_logs = dict()

    for user in tqdm(users.where(lambda u: (u.city is not None) and (u.deleted_at is None) and (u.shahr_id is None)),
                     unit=' user', ncols=100):
        suspected_city = Shahr.select().where(
            lambda s: (s.shahr_type == 0) and (s.name == str(user.city).strip())).first()
        if suspected_city is not None:
            if dry:
                user.shahr_id = suspected_city.id
        elif str(user.city).strip() in cities.keys():
            if dry:
                user.shahr_id = cities[str(user.city).strip()]
        elif str(user.city).strip() in garbage or str(user.city).strip().isnumeric() or str(user.city).strip() == len(
                str(user.city).strip()) * str(user.city).strip()[0]:
            if dry:
                user.city = None
        else:
            city_logs.setdefault(user.city, 0)
            city_logs[user.city] += 1
    if not dry:
        cities_ordered = OrderedDict(sorted(city_logs.items(), key=lambda item: item[1], reverse=True))
        pprint(cities_ordered)
        print('distinct city names not indexed:', len(city_logs))
        print('total city names not indexed:', sum(city_logs.values()))


db.bind(**config)
db.generate_mapping(create_tables=False)

run(True)
run()
