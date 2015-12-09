import datetime
import vk
from sqlalchemy import create_engine, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, Integer, String, Date

api = vk.API(vk.Session())

engine = create_engine('sqlite:///:memory:')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base = declarative_base()

occupations = {
    '': -1,
    'work': 0,
    'school': 1,
    'university': 2
}


user_post_link = Table("user_post_link", Base.metadata,
                       Column("user", ForeignKey("users.id"), primary_key=True),
                       Column("post", ForeignKey("posts.id"), primary_key=True))


class Post(Base):
    __tablename__ = "posts"
    id = Column(Integer, primary_key=True)
    text = Column(String)
    date = Column(Integer)
    user_id = Column(Integer, ForeignKey('users.id'))

    def __init__(self, post):
        self.text = str(post.get('text', ''))
        self.date = int(post.get('date', -1))


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

    posts = relationship(Post, backref="users")

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



Base.metadata.create_all(engine)

users = api.users.get(
    user_ids=list(range(1300, 1400)),
    fields='sex, bdate, city, country, education, counters, occupation, relation, personal'
)

users_bd = [User(user) for user in users if user.get('deactivated', None) is None]
print(len(users_bd))
session.add_all(users_bd)
session.commit()

user1340 = session.query(User).filter_by(id=1340).all()[0]

posts = api.wall.get(
    owner_id=1340,
    count=1,
    filter='owned',
    version=5.40
)


posts_bd = [Post(post) for post in posts[1:]]
print(posts[0])
user1340.posts_count = posts[0]
user1340.posts = posts_bd
session.add_all(posts_bd)
session.commit()

print("-----------------")

for user in session.query(User).filter_by(alcohol=-1).all():
    print(user.last_name, user.first_name, user.posts)
