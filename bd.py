from sqlalchemy import create_engine, ForeignKey,  Boolean, Float, Text
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, Integer, String
from sqlalchemy.pool import SingletonThreadPool

DATABASE = {
    'drivername': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'username': 'postgres',
    'password': '1234',
    'database': 'vk_grabber'
}

engine = create_engine(URL(**DATABASE),
                       echo=False,
                       pool_size=10)


def get_session():
    Session = sessionmaker()
    Session.configure(bind=engine)
    return Session()

occupations = {
    '': -1,
    'work': 0,
    'school': 1,
    'university': 2
}

Base = declarative_base()


class Setting(Base):
    __tablename__ = "settings"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    int_value = Column(Integer)
    str_value = Column(String)
    bool_value = Column(Boolean)
    float_value = Column(Float)

    def __init__(self, key, value):
        self.key = key
        if isinstance(value, int):
            self.int_value = value
        elif isinstance(value, str):
            self.str_value = value
        elif isinstance(value, bool):
            self.bool_value = value
        elif isinstance(value, float):
            self.float_value = value
        else:
            self.str_value = str(value)

    def get_value(self):
        for item in [self.int_value, self.str_value, self.bool_value, self.float_value]:
            if not (item is None):
                return item

        return None


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)

    sex = Column(Integer)
    bdate = Column(String)

    city = Column(Integer)
    country = Column(Integer)

    university = Column(Integer)
    faculty = Column(Integer)

    followers = Column(Integer)
    photos = Column(Integer)
    videos = Column(Integer)
    albums = Column(Integer)
    audios = Column(Integer)
    posts_count = Column(Integer)

    occupation = Column(Integer)
    relation = Column(Integer)

    life_main = Column(Integer)
    alcohol = Column(Integer)
    political = Column(Integer)
    smoking = Column(Integer)
    religion = Column(String)
    people_main = Column(Integer)

    posts = relationship("Post", backref="users")
    post_loaded = Column(Boolean)

    def __init__(self, user):
        self.id = user.get('uid', -1)
        self.first_name = user.get('first_name', "")
        self.last_name = user.get('last_name', "")
        self.religion = dict(user.get('personal', dict())).get('religion', "")

        self.bdate = user.get('bdate', '')

        self.sex = user.get('sex', -1)
        self.city = user.get('city', -1)
        self.country = user.get('country', -1)
        self.university = user.get('university', -1)
        self.faculty = user.get('faculty', -1)

        self.followers = dict(user.get('counters', dict())).get('followers', -1)
        self.photos = dict(user.get('counters', dict())).get('photos', -1)
        self.videos = dict(user.get('counters', dict())).get('videos', -1)
        self.albums = dict(user.get('counters', dict())).get('albums', -1)
        self.audios = dict(user.get('counters', dict())).get('audios', -1)

        self.occupation = occupations[user.get('occupation', dict()).get('type', "")]
        self.relation = dict(user.get('personal', dict())).get('relation', -1)
        self.life_main = dict(user.get('personal', dict())).get('life_main', -1)
        self.alcohol = dict(user.get('personal', dict())).get('alcohol', -1)
        self.political = dict(user.get('personal', dict())).get('political', -1)
        self.smoking = dict(user.get('personal', dict())).get('smoking', -1)
        self.people_main = dict(user.get('personal', dict())).get('people_main', -1)

        self.post_loaded = False


class Post(Base):
    __tablename__ = "posts"
    id = Column(Integer, primary_key=True)
    text = Column(String)
    date = Column(Integer)
    user_id = Column(Integer, ForeignKey('users.id'))

    def __init__(self, post):
        self.text = str(post.get('text', ''))
        self.date = int(post.get('date', -1))

Base.metadata.create_all(bind=engine)
