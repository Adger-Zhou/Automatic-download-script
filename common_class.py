# encoding:utf-8

import sys
import os

sys.path.append(os.path.dirname(__file__))

from ftplib import FTP, FTP_TLS
from oss2 import Auth, Bucket, Service, BucketIterator, models
import datetime
import re
import hashlib
import logging
import redis
import time

reload(sys)
sys.setdefaultencoding('utf-8')


class RedisPool(object):
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db

    def redis_client(self):
        try:
            redis_cli = redis.Redis(host=self.host, port=self.port, db=self.db)
            redis_cli.client_list()
            return redis_cli
        except redis.ConnectionError as e:
            return False


class MyuFtpRemote(object):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ftp = None

    def ftp_coon(self):
        try:
            self.ftp.close()
        except Exception as e:
            pass
        ftp = FTP()
        retry_time = 0
        while retry_time < 3:
            try:
                ftp.connect(self.host, self.port)
                ftp.login(self.username, self.password)
                self.ftp = ftp
                break
            except Exception as e:
                self.ftp = None
                try:
                    ftps = FTP_TLS(self.host)
                    ftps.set_pasv(True)
                    ftps.login(self.username, self.password)
                    ftps.prot_p()

                    self.ftp = ftps
                    break
                except Exception as e:
                    self.ftp = None
            finally:
                if self.ftp:
                    return self.ftp
                retry_time += 1
                time.sleep(5 ** retry_time)

    def get_ftp(self):
        if self.ftp:
            return self.ftp
        else:
            return self.ftp_coon()


class MyOss(object):
    bucket_acl_private = models.BUCKET_ACL_PRIVATE

    def __init__(self, access_key_id, access_key_secret, end_point):
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.end_point = end_point
        self.auth = Auth(self.access_key_id, self.access_key_secret)
        self.service = Service(self.auth, self.end_point)

    def _get_bucket_list(self, service):
        return [b.name for b in BucketIterator(service)]

    def get_bucket_obj(self, bucket_name):
        """
        bucket存在时,修改权限，不存在时创建，权限是私有
        :param bucket_name:
        :return: bucket object
        """
        # bucket_acl = self.bucket_acl_private
        bucket = Bucket(self.auth, self.end_point, bucket_name)
        # if bucket_name not in self._get_bucket_list(self.service):
        #     bucket.create_bucket(bucket_acl)
        # else:
        #     if bucket.get_bucket_acl().acl() != bucket_acl:
        #         bucket.put_bucket_acl(bucket_acl)
        return bucket

    def _get_url(self, bucket, obj_name):
        return bucket.sign_url('GET', obj_name, 3600)


def format_datetime(data_time=None):
    if data_time:
        return data_time.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_new_name(video_name, owner_id, file_size):
    r_string = r"[\\\:\/\(\)\|\*\<\>\s]"
    video_name = re.sub(r_string, "", video_name)
    video_list = video_name.split(".")
    if len(video_list) == 2:
        return "%s_%s_%s.%s" % (video_list[0], str(owner_id), str(int(file_size)), video_list[1])
    else:
        return "%s_%s_%s" % (video_list[0], str(owner_id), str(int(file_size)))


def get_new_url(video_new_name):
    video_info = video_new_name.split(".")
    m2 = hashlib.md5()
    m2.update(video_info[0])
    if len(video_info) == 2:
        return m2.hexdigest() + "." + video_info[1]
    else:
        return m2.hexdigest()


def return_format_size(total_size):
    g = 1073741824
    m = 1048576
    k = 1024
    if total_size >= g:
        g_num = total_size / g
        return "%sG" % g_num
    elif total_size >= m:
        m_num = total_size / m
        return "%sM" % m_num
    elif total_size >= k:
        k_num = total_size / k
        return "%sKb" % k_num
    else:
        return "%s b" % total_size


def return_format_time(total_time):
    d = 86400
    h = 3600
    m = 60
    if total_time >= d:
        d_num = total_time / d
        return u"%s天" % d_num
    elif total_time >= h:
        h_num = total_time / h
        return u"%s小时" % h_num
    elif total_time > m:
        m_num = total_time / m
        return u"%s分钟" % m_num
    else:
        return u"%s秒" % total_time


def get_file_logger(name, format='%(asctime)s %(levelname)s\n%(message)s\n', level=logging.DEBUG):
    log = logging.getLogger(name)
    if not log.handlers:
        handler = logging.FileHandler('%s' % name)
        formatter = logging.Formatter(format)
        handler.setFormatter(formatter)
        log.addHandler(handler)
        log.setLevel(level)
    return log
