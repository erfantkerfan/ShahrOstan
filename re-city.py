from pony.orm import *
from pymongo import MongoClient
from tqdm import tqdm

from config import *

db_mysql = Database()


class Users(db_mysql.Entity):
    id = PrimaryKey(int)
    firstName = Optional(str)
    lastName = Optional(str)
    mobile = Required(str)
    nationalCode = Required(str)
    province = Optional(str)
    shahr_id = Optional(int)
    city = Optional(str, nullable=True)
    deleted_at = Optional(str)


class Shahr(db_mysql.Entity):
    id = PrimaryKey(int)
    ostan_id = Required(int)
    shahr_type = Required(int)
    name = Required(str)


class Ostan(db_mysql.Entity):
    id = PrimaryKey(int)
    name = Required(str)


@db_session
def correct_users():
    global tnot, notnot
    notnot = 0
    tnot = 0
    for user_mongo in tqdm(cursor, unit=' users', ncols=200):
        user_mysql = Users.select().where(lambda u: (u.id == user_mongo['_id'])).first()
        if user_mysql.shahr_id is None:
            user_mongo['city'] = None
            user_mongo['province'] = None
        else:
            shar = Shahr.select().where(lambda s: (s.id == user_mysql.shahr_id)).first()
            ostan = Ostan.select().where(lambda o: (o.id == shar.ostan_id)).first()
            user_mongo['city'] = shar.name
            user_mongo['province'] = ostan.name
            collection.replace_one({"_id": user_mongo["_id"]}, user_mongo)

    cursor.close()


db_mysql.bind(**config_mariadb)
db_mysql.generate_mapping(create_tables=False)

client = MongoClient(**config_mongodb)
db_mongo = client.get_database(config_mongodb['name'])
collection = db_mongo.get_collection('users')
cursor = collection.find({}, no_cursor_timeout=True)

correct_users()
