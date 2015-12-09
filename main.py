import logging
from threading import Thread
from time import sleep

import vk
from vk.exceptions import VkAPIError

from bd import *

FORMAT = '%(levelname)-7s: %(lineno)d: %(name)-10s %(asctime)-15s %(message)s'
logging.basicConfig(datefmt='%d.%m.%Y %I:%M:%S.%p',
                    format=FORMAT,
                    level=logging.INFO)

is_run = True


class SessionDBThread(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("SessionDBThread")
        self.session = get_session()

    def run(self):
        while is_run:
            sleep(1)

    def get_setting(self, key):
        pass

    def set_setting(self, key, value):
        pass


class VKUserGetThread(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("VKUserGetThread")

        self.api = vk.API(vk.Session())
        self.session = get_session()
        self.last_added_user = self.session.query(Setting).filter_by(key="last_added_user").first()
        self.chunk = 1000
        if self.last_added_user is None:
            self.last_added_user = Setting("last_added_user", 0)
            self.session.add(self.last_added_user)
            self.session.commit()

    def run(self):
        while is_run:
            cur_uid = self.last_added_user.int_value
            users = None
            while users is None:
                try:
                    users = self.api.users.get(
                        user_ids=list(range(cur_uid, cur_uid + self.chunk)),
                        fields='sex, bdate, city, country, education, counters, occupation, relation, personal'
                    )
                except VkAPIError as e:
                    self.logger.warn("Exception {} as vk.api.users.get", e)
                    if e.code == 6 or e.code == 14:
                        sleep(1)
                    else:
                        break

            if users is None:
                continue

            users_bd = [User(user) for user in users if user.get('deactivated', None) is None]
            self.session.add_all(users_bd)
            self.last_added_user.int_value = cur_uid + self.chunk
            self.session.commit()

            self.logger.info("Added {} users, current uid: {}".format(
                             len(users_bd), cur_uid))


class VKPostsGetThread(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("VKPostsGetThread")
        self.api = vk.API(vk.Session())
        self.session = get_session()

    def run(self):
        while is_run:
            users = self.session.query(User).filter_by(post_loaded=False).limit(100)
            for user in users:
                if not is_run:
                    break

                user.post_loaded = True

                posts = None
                while posts is None:
                    try:
                        self.logger.info("Get posts for id{}".format(user.id))
                        posts = self.api.wall.get(
                            owner_id=user.id,
                            count=100,
                            filter='owned',
                            version=5.40
                        )
                    except VkAPIError as e:
                        self.logger.warn("Exception {} as vk.api.wall.get".format(e))
                        if e.code == 6 or e.code == 14:
                            sleep(1)
                        else:
                            break

                if posts is None:
                    continue

                posts_bd = [Post(post) for post in posts[1:] if not post.get('text', '') is '']
                user.posts_count = posts[0]
                user.posts = posts_bd
                self.session.add_all(posts_bd)
                self.session.commit()

                self.logger.info("Id{}, found {} posts".format(user.id, len(posts_bd)))


users_get = VKUserGetThread()
posts_get = VKPostsGetThread()

users_get.start()
posts_get.start()

try:
    while 1:
        sleep(1)
except KeyboardInterrupt:
    is_run = False
    print("KeyInterrupt pressed")

users_get.join()
posts_get.join()
