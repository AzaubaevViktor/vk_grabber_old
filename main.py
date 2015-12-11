import logging
from random import randint
from threading import Thread
from time import sleep

import sys
import vk
from vk.exceptions import VkAPIError

from bd import *

from pymystem3 import Mystem

FORMAT = '%(levelname)-7s: %(lineno)d: %(name)-10s %(asctime)-15s %(message)s'
logging.basicConfig(datefmt='%d.%m.%Y %I:%M:%S.%p',
                    format=FORMAT,
                    level=logging.INFO)

is_run = True


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
                except Exception as e:
                    self.logger.warn("Exception {} as vk.api.wall.get".format(e))
                    self.logger.warn("Waiting second...")
                    sleep(1)

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
            users = self.session.query(User).filter_by(post_loaded=False).offset(randint(1, 10000000)).limit(100)
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
                            filter='owner',
                            version=5.40
                        )
                    except VkAPIError as e:
                        self.logger.warn("Exception {} as vk.api.wall.get".format(e))
                        if e.code == 6 or e.code == 14:
                            self.logger.warn("Waiting second...")
                            sleep(1)
                        else:
                            break
                    except Exception as e:
                        self.logger.warn("Exception {} as vk.api.wall.get".format(e))
                        self.logger.warn("Waiting second...")
                        sleep(1)

                if posts is None:
                    continue

                posts_bd = [Post(post) for post in posts[1:] if not post.get('text', '') is '']
                user.posts_count = posts[0]
                user.posts = posts_bd
                self.session.add_all(posts_bd)
                self.session.commit()

                self.logger.info("Id{}, found {} posts".format(user.id, len(posts_bd)))


class LemmatizingThread(Thread):
    available_char = " -йцукенгшщзхъфывапролджэячсмитьбюё"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("LemmatizingThread")
        self.session = get_session()
        self.mystem = Mystem()

    def run(self):
        while is_run:
            posts = self.session.query(Post).filter(Post.lemmas is not None).limit(1000)
            count = 0
            for post in posts:
                if not is_run:
                    break
                post.lemmas = self._string_clear(post.text)
                count += 1
            self.session.commit()

            self.logger.info("Handled {} posts".format(count))

    def _string_clear(self, s: str):
        if "" == s:
            return []

        _s1 = ""
        # удалить <br>
        s = s.replace("<br>", " ")
        s = s.lstrip().rstrip().lower()
        # Удалить лишние символы
        for c in s:
            if c.isalpha() or c in self.available_char:
                _s1 += c
            else:
                _s1 += " "

        # Удалить лишние пробелы и нормализовать слова
        # _s1 = [self.mystem.lemmatize(x)[0].rstrip().lstrip() for x in _s1.split(" ") if x and x != "-"]
        # _s1 = [x for x in _s1 if x != "-"]
        _s1 = [x for x in [x.rstrip().lstrip() for x in _s1.split(" ") if x and x != "-"] if x != "-"]
        _s1 = "".join(self.mystem.lemmatize(" ".join(_s1))[:-1])

        return _s1

if __name__ == "__main__":

    users_get = None
    if sys.argv[0] != "onlyposts":
        users_get = VKUserGetThread()
        users_get.start()

    posts_get = VKPostsGetThread()
    posts_get.start()

    lemmatizing = LemmatizingThread()
    lemmatizing.start()

    try:
        while 1:
            sleep(1)
    except KeyboardInterrupt:
        is_run = False
        print("KeyInterrupt pressed")

    if sys.argv[0] != "onlyposts":
        users_get.join()
    posts_get.join()
    lemmatizing.join()
