# encoding:utf-8

import sys
import os

sys.path.append(os.path.dirname(__file__))

# Global
DEBUG = False
ENV = 'stagings'
# celery
BROKER_URL = 'redis://127.0.0.1:6379/0'
CELERY_ROUTES = {'celery_tasks.video.tasks.analysis_video': {'queue': 'download'}}
CELERY_ACKS_LATE = True
CELERY_IGNORE_RESULT = True
CELERY_DISABLE_RATE_LIMITS = True
# OSS
ACCESS_KEY_ID = ""
ACCESS_KEY_SECRET = ""
END_POINT = "oss-cn-beijing-internal.aliyuncs.com"
END_POINT_PUBLIC = "oss-cn-beijing.aliyuncs.com"
VIDEO_BUCKET_NAME = ""
IMAGE_BUCKET_NAME = ""
PART_SIZE = 104857600  # 分片大小
UPLOAD_CACHE = '/home/adger/logs'
# Download
THREAD_MAX = 5  # 最大线程数量
RETRY_TIMES = 3  # 异常重试次数
VIDIBLE_KEY = ""  # vidible授权码
LOG_FOLDER = "/home/adger/logs"
MRSS_DIR = "/home/adger/logs/mrss_dir"
REMOTE_DIR = "/home/adger/logs/remote_dir"
LOCAL_FTP_DIR = "/home/adger/ftp_home"
LOCAL_VIDEO_CACHE = "/home/adger/ftp_home"
XML_PATH="/home/adger/logs/mrss_dir"
BUFFER_SIZE = 131072
VIDEO_FORMAT = ["mp4", "mov", "avi"]
######redis
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 8
# BACKEND
SERVER = "http://127.0.0.1:8000/api/"
GETDUP = "getdup/"
SOURCE_URL = "get_source_list/"
UPDATE_STATUS = "update-rss-ftp-status/"
WRONG_SLEEP = 5
# 保存video数据API
VIDEO_SAVE_API = "save_video_info/"

#########
DOWNLOAD_FAILED = "download_failed.log"  # 下载失败视频列表
WRONG_VIDEO = "wrong_video.log"  # 文件损坏列表
COMPLETE_WRONG = "complete_wrong.log"  # 分片合并失败列表
EXIST_VIDEO = "exist_video.log"  # 已存在列表
CELERY_WRONG = "celery_wrong.log"  # 调用celery失败
