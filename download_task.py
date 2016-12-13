# encoding:utf-8

import os
import sys
import feedparser
import datetime
import threading
import time
import shutil
import requests
import json
from oss2 import resumable_upload, ResumableStore
from celery import Celery

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
sys.path.append(os.path.dirname(__file__))

import download_settings as setting
from common_class import get_file_logger, MyuFtpRemote, get_new_name, get_new_url, MyOss, RedisPool, return_format_size, \
    return_format_time

app = Celery()
app.config_from_object('download_settings')
download_failed = get_file_logger(
    os.path.join(setting.LOG_FOLDER, "download_failed_%s.log" % datetime.date.today().strftime("%Y_%m_%d")))
download_success = get_file_logger(
    os.path.join(setting.LOG_FOLDER, "download_success_%s.log" % datetime.date.today().strftime("%Y_%m_%d")))
video_exists = get_file_logger(
    os.path.join(setting.LOG_FOLDER, "download_exists_%s.log" % datetime.date.today().strftime("%Y_%m_%d")))
send_task_wrong = get_file_logger(
    os.path.join(setting.LOG_FOLDER, "send_task_wrong_%s.log" % datetime.date.today().strftime("%Y_%m_%d")))
save_video_wrong = get_file_logger(
    os.path.join(setting.LOG_FOLDER, "save_video_wrong_%s.log" % datetime.date.today().strftime("%Y_%m_%d")))

myLog = get_file_logger(
    os.path.join(setting.LOG_FOLDER, "download_%s.log" % datetime.date.today().strftime("%Y_%m_%d")))
mutex = threading.Lock()
if setting.ENV == "production":
    end_point = setting.END_POINT
else:
    end_point = setting.END_POINT_PUBLIC
myOss = MyOss(setting.ACCESS_KEY_ID, setting.ACCESS_KEY_SECRET, end_point)
video_index_dict = {"video_index": 0}
video_total_dict = {"video_total": 0}
down_size_dict = {"down_size": 0}
down_time_dict = {"down_time": 0}
down_failed = {"failed": 0}
down_success = {"success": 0}
down_exists = {"exists": 0}
bucket_video = myOss.get_bucket_obj(setting.VIDEO_BUCKET_NAME)


def get_source_tuple():
    """
    获取下载源：mrss,ftp,local
    :return: LIST
    """
    remote_dict = {}
    mrss_dict = {}
    getsource_url = "%s%s" % (setting.SERVER, setting.SOURCE_URL)
    myLog.info(u"开始获取下载源:%s" % getsource_url)
    retry_time = 1
    while retry_time <= setting.RETRY_TIMES:
        try:
            result = requests.get(getsource_url, stream=True, verify=False, timeout=120)
            if result.status_code == requests.codes.ok:
                content = json.loads(result.content)
                mrss_dict = content.get("mrss_dict", {})
                remote_dict = content.get("remote_dict", {})
                break
            else:
                myLog.warning(u'url请求失败:%s,%s秒后重试' % (getsource_url, setting.WRONG_SLEEP ** retry_time))
                time.sleep(setting.WRONG_SLEEP ** retry_time)
                retry_time += 1
        except Exception as e:
            myLog.warning(u'url请求失败:%s,%s秒后重试' % (getsource_url, setting.WRONG_SLEEP ** retry_time))
            myLog.warning(e)
            time.sleep(setting.WRONG_SLEEP ** retry_time)
            retry_time += 1
    return (mrss_dict, remote_dict)


def get_video_size(down_link):
    retry_time = 1
    while retry_time <= setting.RETRY_TIMES:
        try:
            meta = requests.get(down_link, stream=True, timeout=60, verify=False)
            if meta.status_code == requests.codes.ok:
                file_size = int(meta.headers.get("Content-Length", 0))
                return file_size
            else:
                myLog.warning(u"%s获取视频大小失败,%s秒后重试" % (down_link, setting.WRONG_SLEEP ** retry_time))
                time.sleep(setting.WRONG_SLEEP ** retry_time)
                retry_time += 1
        except Exception as e:
            myLog.warning(u"%s获取网络链接失败,%s秒后重试" % (down_link, setting.WRONG_SLEEP ** retry_time))
            myLog.warning(e)
            time.sleep(setting.WRONG_SLEEP ** retry_time)
            retry_time += 1


def feedparser_xml(data_info, transcode_template_list, ):
    down_dict = {}
    mrss_url = data_info.get("mrss_url", "")
    owner_id = str(data_info.get("owner_id", ""))
    mrss_url_total = 0
    mrss_url_success = 0
    myLog.info(u"开始解析mrss源:%s" % mrss_url)
    xml_doc = None
    retry_time = 0
    while retry_time < setting.RETRY_TIMES:
        try:
            xml_doc = feedparser.parse(mrss_url)
            break
        except Exception as e:
            retry_time += 1
            myLog.warning(u'解释mrss源%s失败,%s后重试' % (mrss_url, setting.WRONG_SLEEP ** retry_time))
            myLog.error(e)
            time.sleep(setting.WRONG_SLEEP ** retry_time)
    if xml_doc:
        for media_item in xml_doc.entries:
            mrss_url_total += 1
            try:
                vidible_id = media_item.vidible_id
            except:
                vidible_id = None
            if vidible_id:
                ############vidible api###########
                api_link = "http://api.vidible.tv/%s/video/%s" % (setting.VIDIBLE_KEY, vidible_id)
                try:
                    meta = requests.get(api_link, stream=True, timeout=120, verify=False)
                except Exception as e:
                    myLog.info(u"API访问失败:%s" % api_link)
                    myLog.warning(e)
                    continue
                try:
                    result = json.loads(meta.content)[0]
                except Exception as e:
                    myLog.info(u"API访问失败:%s" % api_link)
                    myLog.warning(e)
                    continue
                if result:
                    title = result.get("name", "")
                    description = result.get("description", "")
                    category_list = result.get("category", ["other"])
                    tag_list = result.get("tags", [])
                    down_link = result.get("originalVideoUrl", "")
                    data_other_info = {"category_list": category_list, "tag_list": tag_list}
                else:
                    continue
            else:
                title = media_item.title
                description = media_item.description
                down_link = media_item.media_content[0].get("url")
                try:
                    category_string = media_item.category
                except:
                    category_string = ""
                try:
                    tag_string = media_item.media_keyword.encode("utf-8")
                except:
                    try:
                        tag_string = media_item.media_keywords.encode("utf-8")
                    except:
                        tag_string = ""
                data_other_info = parse_category_tags(category_string, tag_string)
            video_name = down_link.split("/")[-1]
            file_size = get_video_size(down_link)
            if file_size:
                video_new_name = get_new_name(video_name, owner_id, file_size=file_size)
                video_url = get_new_url(video_new_name)
                file_exists_status = file_exists(video_url)
                if file_exists_status is not None:
                    if not file_exists_status:
                        data_l = {
                            "source": "mrss",
                            "owner": owner_id,
                            "title": title,
                            "description": description,
                            "plays": 0,
                            "video_new_name": video_new_name,
                            "down_link": down_link,
                            "file_size": file_size,
                            "file_path": video_url,
                            "transcode_template_list": transcode_template_list,
                        }
                        data_l.update(data_other_info)
                        down_dict[video_url] = data_l
                        mrss_url_success += 1
                    else:
                        continue
                else:
                    continue
        xml_doc.clear()
        myLog.info(u"mrss源:%s,视频解析总数:%s,视频解析成功数:%s" % (mrss_url, mrss_url_total, mrss_url_success))
    else:
        myLog.warning(u"解析源:%s失败" % mrss_url)
    down_info_list = removed_uplicate_records(down_dict)
    down_list_length = len(down_info_list)
    video_total_dict["video_total"] = video_total_dict.get("video_total", 0) + down_list_length
    myLog.info(u"mrss源:%s解析新视频个数:%s" % (mrss_url, down_list_length))
    return down_info_list


def local_ftp_scanning(username, transcode_template_list, owner_id, local_ftp_dir, is_ctv):
    down_dict = {}
    total_num = 0
    success_num = 0
    file_names = os.listdir(local_ftp_dir)
    for file_name in file_names:
        full_name = os.path.join(local_ftp_dir, file_name)
        if os.path.isdir(full_name):
            down_tuple_next = local_ftp_scanning(username, transcode_template_list, owner_id, full_name, is_ctv)
            total_num += down_tuple_next[0]
            success_num += down_tuple_next[1]
            down_dict.update(down_tuple_next[2])
        else:
            total_num += 1
            file_name_info = full_name.split(".")
            if file_name_info[-1] in setting.VIDEO_FORMAT:
                file_size = os.stat(full_name).st_size
                video_new_name = get_new_name(file_name, owner_id=owner_id, file_size=file_size)
                video_url = get_new_url(video_new_name)
                file_exists_status = file_exists(video_url)
                if file_exists_status is not None:
                    if not file_exists_status:
                        xml_name = "%s.%s" % (file_name, "xml")
                        if xml_name in file_names:
                            data_video = {
                                "source": "ftp",
                                "owner": owner_id,
                                "video_full_path": full_name,
                                "xml_full_path": os.path.join(local_ftp_dir, xml_name),
                                "file_size": file_size,
                                "video_new_name": video_new_name,
                                "file_path": video_url,
                                "transcode_template_list": transcode_template_list,
                                "is_ctv": is_ctv,
                                "username": username
                            }
                            down_dict[video_url] = data_video
                            success_num += 1
                        else:
                            myLog.warning(u"供应商id:%s,本地视频文件%s没有对应的xml文件" % (owner_id, full_name))
                    else:
                        continue
                else:
                    continue
            else:
                myLog.warning(u'视频:%s,格式不支持')
    return (total_num, success_num, down_dict)


def remote_ftp_scanning(owner_id, transcode_template_list, myFtpRemote, is_ctv, dirpath="/"):
    """
    递归获取文件路劲
    :param:@dirpath:路径
    :return:[{}]
    """
    down_dict = {}
    total_num = 0
    success_num = 0
    ftp = myFtpRemote.get_ftp()
    if not ftp:
        myLog.warning(u"ftp登录失败HOST:%s,Username:%s" % (myFtpRemote.host, myFtpRemote.username))
    else:
        try:
            file_list = ftp.nlst(dirpath)
        except Exception as e:
            myLog.warning(u"ftp目录:%s扫描失败,HOST:%s,Username:%s" % (dirpath, myFtpRemote.host, myFtpRemote.username))
            myLog.error(e)
            myFtpRemote.ftp = None
            file_list = []
        for file_path in file_list:
            # 判断是否是目录
            try:
                ftp.cwd(file_path)
                is_dir = True
            except:
                is_dir = False
            if is_dir:
                down_tuple_next = remote_ftp_scanning(owner_id, transcode_template_list, myFtpRemote, is_ctv, file_path)
                total_num += down_tuple_next[0]
                success_num += down_tuple_next[1]
                down_dict.update(down_tuple_next[2])
            else:
                total_num += 1
                try:
                    file_size = ftp.size(file_path)
                except Exception as e:
                    myLog.warning(
                        u"ftp文件:%s获取size失败,HOST:%s,Username:%s" % (file_path, myFtpRemote.host, myFtpRemote.username))
                    myFtpRemote.ftp = None
                    myLog.error(e)
                    continue
                file_name = file_path.split("/")[-1]
                file_name_info = file_name.split(".")
                if file_name_info[-1] in setting.VIDEO_FORMAT:
                    video_new_name = get_new_name(file_name, owner_id=owner_id, file_size=file_size)
                    video_url = get_new_url(video_new_name)
                    file_exists_status = file_exists(video_url)
                    if file_exists_status is not None:
                        if not file_exists_status:
                            xml_full_name = "%s.%s" % (file_path, "xml")
                            if xml_full_name in file_list:
                                data = {
                                    "source": "ftp",
                                    "is_ctv": is_ctv,
                                    "owner": owner_id,
                                    "host": myFtpRemote.host,
                                    "port": myFtpRemote.port,
                                    "username": myFtpRemote.username,
                                    "passwd": myFtpRemote.password,
                                    "video_full_path": file_path,
                                    "xml_full_path": xml_full_name,
                                    "file_size": file_size,
                                    "video_new_name": video_new_name,
                                    "file_path": video_url,
                                    "transcode_template_list": transcode_template_list
                                }
                                down_dict[video_url] = data
                                success_num += 1
                            else:
                                myLog.warning(u"供应商id:%s,sftp视频文件%s没有对应的xml文件" % (owner_id, file_path))
                        else:
                            continue
                    else:
                        continue
                else:
                    myLog.warning(u"视频文件:%s格式不支持" % file_path)
    return (total_num, success_num, down_dict)


def ftp_parser(source_dict):
    owner_id = source_dict.get("owner_id")
    transcode_template_list = source_dict.get("transcode_template_list")
    is_ctv = source_dict.get("is_ctv")
    username = source_dict.get("username")
    if is_ctv:
        local_ftp_dir = os.path.join(setting.LOCAL_FTP_DIR, username)
        myLog.info(u'开始解析本地源:%s' % local_ftp_dir)
        total_num, success_num, down_dict = local_ftp_scanning(username, transcode_template_list, owner_id,
                                                               local_ftp_dir, is_ctv)
        myLog.info(u"本地源:%s,解析视频总数:%s,解析成功视频数:%s" % (username, total_num, success_num))
        download_list = removed_uplicate_records(down_dict)
        down_list_length = len(download_list)
        myLog.info(u"Local源:%s解析新视频个数:%s" % (username, down_list_length))

    else:
        passwd = source_dict.get("passwd")
        host = source_dict.get("host")
        port = source_dict.get("port") or 21
        myFtpRemote = MyuFtpRemote(host, port, username, passwd)
        myLog.info(u'开始解析remote源host:%s,username:%s' % (host, username))
        total_num, success_num, down_dict = remote_ftp_scanning(owner_id, transcode_template_list, myFtpRemote, is_ctv)
        myLog.info(u"remote源:(host:%s,username:%s),解析视频总数:%s,解析成功视频数:%s" % (host, username, total_num, success_num))
        try:
            myFtpRemote.ftp.quit()
            myFtpRemote.ftp.close()
        except:
            pass

        download_list = removed_uplicate_records(down_dict)
        down_list_length = len(download_list)
        myLog.info(u"Remote源(host:%s,username:%s)解析新视频个数:%s" % (host, username, down_list_length))
    video_total_dict["video_total"] = video_total_dict.get("video_total", 0) + down_list_length
    return download_list


def get_down_info_list(source_dict, source):
    """
    根据不同的来源使用不同的解析方式
    :param source_dict:
    :return:
    """
    owner_id = source_dict.get("owner_id")
    transcode_template_list = source_dict.get("transcode_template_list")
    if source == "mrss":
        mrss_url = source_dict.get("mrssurl")
        feedparser_xml_data = {"owner_id": owner_id, "source": "mrss", "mrss_url": mrss_url}
        download_list = feedparser_xml(feedparser_xml_data, transcode_template_list)
    else:
        download_list = ftp_parser(source_dict)

    return download_list


def removed_uplicate_records(down_dict):
    """
    去掉重复记录
    :param down_dict:
    :return:
    """
    key_list = down_dict.keys()
    video_dup_list = get_dup_list(key_list)
    for item in video_dup_list:
        del down_dict[item]
    return down_dict.values()


def get_dup_list(key_list):
    """
    获取重复数据列表
    :param down_dict:
    :return: []
    """
    getdup_url = "%s%s" % (setting.SERVER, setting.GETDUP)
    video_dup_list = []
    retry_time = 1
    while retry_time <= setting.RETRY_TIMES:
        try:
            result = requests.post(getdup_url, data={"video_name_list": key_list}, verify=False, timeout=120)
            content = json.loads(result.content)
            video_dup_list = content.get("video_name_list")
            break
        except Exception as e:
            sleep_times = setting.WRONG_SLEEP ** retry_time
            myLog.warning(u"重复数据API:%s请求失败,%s后重试" % (getdup_url, sleep_times))
            myLog.error(e)
            time.sleep(sleep_times)
            retry_time += 1
    return video_dup_list


def get_bucket_video(video_url):
    retry_time = 1
    global bucket_video
    while retry_time <= setting.RETRY_TIMES:
        try:
            if not bucket_video:
                bucket_video = myOss.get_bucket_obj(setting.VIDEO_BUCKET_NAME)
            bucket_video.object_exists(video_url)
        except Exception as e:
            bucket_video = None
            myLog.warning(
                u"建立bucket:%s链接失败，%s秒后尝试重连" % (setting.VIDEO_BUCKET_NAME, setting.WRONG_SLEEP ** retry_time))
            myLog.warning(e)
            time.sleep(setting.WRONG_SLEEP ** retry_time)
            retry_time += 1


def get_file_obj(down_link, offset):
    retry_time = 1
    while retry_time <= setting.RETRY_TIMES:
        try:
            headers = {'Range': 'bytes=%d-' % offset}
            webPage = requests.get(down_link, stream=True, headers=headers, timeout=120, verify=False)
            return webPage
        except Exception as e:
            myLog.warning(u"urllib链接:%s错误,%s秒后准备重试" % (down_link, setting.WRONG_SLEEP ** retry_time))
            myLog.warning(e)
            time.sleep(setting.WRONG_SLEEP ** retry_time)
            retry_time += 1


def urllib_post(data):
    save_api = "%s%s" % (setting.SERVER, setting.VIDEO_SAVE_API)
    try:
        req = requests.post(save_api, data=data, verify=False, timeout=120)
        content = json.loads(req.content)
    except Exception as e:
        content = None
        myLog.warning(u"调用储存API失败:%s" % save_api)
        myLog.warning(e)
    if content:
        video_id = content.get("video_id")
        myLog.info(u"储存对象:%s保存数据成功,video_id:%s" % (data.get("file_path"), video_id))
        celery_data = {"video_id": video_id, "file_path": data.get("file_path"),
                       "transcode_template_list": data.get("transcode_template_list"),
                       "owner": data.get("owner")}
        try:
            pass
            app.send_task('celery_tasks.video.tasks.analysis_video', args=[celery_data, True])
        except Exception as e:
            myLog.warning(u"储存对象:%s发送视频解析任务失败" % data.get("file_path"))
            myLog.warning(e)
    else:
        myLog.info(u"储存对象:%s保存数据失败" % (data.get("file_path")))


def mrss_download(video_url, down_link, file_size):
    """
    下载mrss文件到本地
    :param video_url:本地文件名称
    :param down_link:下载视频链接
    :param file_size:视频大小
    :return:下载状态
    """
    result = False
    retry_time = 1
    local_path = os.path.join(setting.MRSS_DIR, video_url)
    try:
        lsize = os.stat(local_path).st_size
    except:
        lsize = 0
    mutex.acquire()
    total_num = video_total_dict.get("video_total", 0)
    index_num = video_index_dict.get("video_index", 0)
    myLog.info(u"视频:%s,开始下载:%s,进度:%s/%s" % (down_link, local_path, index_num, total_num))
    mutex.release()
    webPage = get_file_obj(down_link, lsize)
    down_size = 0
    datatime_start = datetime.datetime.now()
    while retry_time <= setting.RETRY_TIMES:
        if not webPage:
            webPage = get_file_obj(down_link, lsize)
        rest_size = file_size - lsize
        buffer_size = min(setting.BUFFER_SIZE, rest_size)
        with open(local_path, 'ab') as file_obj:
            try:
                for chunk in webPage.iter_content(chunk_size=buffer_size):
                    file_obj.write(chunk)
                    data_length = len(chunk)
                    lsize += data_length
                    down_size += data_length
                webPage.close()
                file_obj.close()
                result = True
                datetime_end = datetime.datetime.now()
                time_seconds = (datetime_end - datatime_start).seconds
                mutex.acquire()
                time_seconds_new = down_time_dict.get("down_time", 0) + time_seconds
                down_time_dict["down_time"] = time_seconds_new
                myLog.info(u"视频:%s下载成功,本地储存:%s,进度:%s/%s" % (down_link, local_path, index_num, total_num))
                mutex.release()
                break
            except Exception as e:
                file_obj.close()
                webPage = None
                sleep_seconds = setting.WRONG_SLEEP ** retry_time
                myLog.warning(
                    u'视频:%s下载失败,本地储存:%s,%s秒后重试,进度:%s/%s' % (down_link, local_path, sleep_seconds, index_num, total_num))
                myLog.warning(e)
                time.sleep(sleep_seconds)
                retry_time += 1

    if result:
        mutex.acquire()
        down_size_dict["down_size"] = down_size_dict.get("down_size", 0) + down_size
        success_num = down_success.get("success", 0) + 1
        down_success["success"] = success_num
        mutex.release()
        res = vodeo_update(video_url, local_path)
        if res and os.path.exists(local_path):
            os.remove(local_path)
        return res
    else:
        mutex.acquire()
        myLog.warning(u'视频:%s下载失败,本地储存:%s,进度:%s/%s' % (down_link, local_path, index_num, total_num))
        download_failed.info("down_link:%s,video_url:%s" % (down_link, local_path))
        failed_num = down_failed.get("failed", 0) + 1
        down_failed["failed"] = failed_num
        mutex.release()


def vodeo_update(video_url, video_full_path):
    retry_time = 1
    result = False
    myLog.info(u"文件:%s开始上传,oss对象:%s" % (video_full_path, video_url))
    global bucket_video
    while retry_time <= setting.RETRY_TIMES:
        if not bucket_video:
            bucket_video = get_bucket_video(video_url)
        try:
            resumable_upload(bucket_video, video_url, video_full_path,
                             store=ResumableStore(root=setting.UPLOAD_CACHE),
                             multipart_threshold=setting.PART_SIZE,
                             part_size=setting.PART_SIZE,
                             num_threads=4)
            myLog.info(u"文件:%s上传成功,oss对象:%s" % (video_full_path, video_url))
            result = True
            break
        except Exception as e:
            bucket_video = None
            sleep_seconds = setting.WRONG_SLEEP ** retry_time
            myLog.warning(u"文件:%s上传oss:%s失败,%s后重试" % (video_full_path, video_url, sleep_seconds))
            myLog.error(e)
            time.sleep(sleep_seconds)
            retry_time += 1
    return result


def remote_choice(data):
    username = data.get("username")
    is_ctv = data.get("is_ctv")
    xml_full_path = data.get("xml_full_path")
    video_url = data.get("file_path")
    video_full_path = data.get("video_full_path")
    res = False  # 判断文件是否存在OSS对象
    video_local_path = video_full_path
    if is_ctv:
        xml_new_path = xml_full_path
        res = vodeo_update(video_url, video_full_path)
    else:
        host = data.get("host")
        port = data.get("port")
        password = data.get("passwd")
        xml_new_path = os.path.join(setting.XML_PATH, "%s.xml" % video_url)
        myFtpRemote = MyuFtpRemote(host, port, username, password)
        xml_down_status = remote_download(xml_new_path, xml_full_path, myFtpRemote, is_xml=True)
        if xml_down_status:
            video_local_path = os.path.join(setting.REMOTE_DIR, video_url)
            video_down_status = remote_download(video_local_path, video_full_path, myFtpRemote)
            if video_down_status:
                res = vodeo_update(video_url, video_local_path)
        try:
            myFtpRemote.ftp.quit()
            myFtpRemote.ftp.close()
        except:
            pass
    if res:
        video_info_other_dict = feedparser_local_xml(xml_new_path)
        data.update(video_info_other_dict)
        if is_ctv and os.path.exists(video_local_path):
            video_file_name = video_local_path.split("/")[-1]
            xml_file_name = xml_full_path.split("/")[-1]
            video_cache_dir_name = "%s_%s" % (username, datetime.date.today().strftime("%Y_%m_%d"))
            video_cache_path = os.path.join(setting.LOCAL_FTP_DIR, video_cache_dir_name)
            if not os.path.exists(video_cache_path):
                os.makedirs(video_cache_path)
            video_file_cache_path = os.path.join(video_cache_path, video_file_name)
            xml_file_cache_path = os.path.join(video_cache_path, xml_file_name)
            shutil.move(video_local_path, video_file_cache_path)
            shutil.move(xml_new_path, xml_file_cache_path)
        elif not is_ctv and os.path.exists(video_local_path):
            os.remove(xml_new_path)
            os.remove(video_local_path)
        urllib_post(data)


def feedparser_local_xml(xml_new_path):
    """
    解析xml文件
    @:param:param1:sftp视频文件夹根目录
    @:param:param2:数据库链接对象
    @:return:None；
    """
    data_dict = {}
    if not os.path.exists(xml_new_path):
        myLog.warning(u"xml文件%s不存在" % xml_new_path)
    else:
        try:
            per = ET.parse(xml_new_path)
            p = per.findall("meta")
            for i in p:
                for j in i.getchildren():
                    attr = j.attrib
                    data_dict[attr.get("name", "")] = attr.get("value", "")
        except Exception as e:
            myLog.warning(u'xml文件:%s解析失败' % xml_new_path)
            myLog.warning(e)
    category_string = data_dict.get("category", "")
    tag_string = data_dict.get("keyword", "")
    if data_dict.get("synopsis", ""):
        data_dict["description"] = data_dict.get("synopsis")
    category_tag_dict = parse_category_tags(category_string, tag_string)
    data_dict.update(category_tag_dict)
    return data_dict


def parse_category_tags(category_string, tag_string):
    category_list = []
    if category_string:
        for category_l in category_string.split(","):
            category_list.append(category_l.strip())
    else:
        category_list.append("other")
    tag_list = []
    if tag_string:
        for tag_l in tag_string.split(","):
            tag_list.append(tag_l.strip())
    return {"category_list": category_list, "tag_list": tag_list}


def remote_download(video_local_path, video_full_path, myFtpRemote, is_xml=False):
    """
    下载mrss文件到本地
    :param video_url:本地文件名称
    :param file_size:视频大小
    :return:下载状态
    """
    if not is_xml:
        mutex.acquire()
        total_num = video_total_dict.get("video_total", 0)
        index_num = video_index_dict.get("video_index", 0) + 1
        video_index_dict["video_index"] = index_num
        myLog.info(u"host:%s,username:%s,视频:%s开始下载:%s,进度:%s/%s" % (
            myFtpRemote.host, myFtpRemote.username, video_full_path, video_local_path, index_num, total_num))
        mutex.release()
    else:
        total_num = ""
        index_num = ""
    result = False
    retry_time = 1
    lsize_start = 0
    datatime_start = datetime.datetime.now()
    while retry_time <= setting.RETRY_TIMES:
        try:
            lsize = os.stat(video_local_path).st_size
            if retry_time == 1:
                lsize_start = lsize
        except:
            lsize = 0
        ftp = myFtpRemote.get_ftp()
        try:
            fp = open(video_local_path, 'ab')
            ftp.retrbinary('RETR %s' % video_full_path, fp.write, setting.BUFFER_SIZE, lsize)
            result = True
            datetime_end = datetime.datetime.now()
            time_seconds = (datetime_end - datatime_start).seconds
            mutex.acquire()
            time_seconds_new = down_time_dict.get("down_time", 0) + time_seconds
            down_time_dict["down_time"] = time_seconds_new
            myLog.info(u"host:%s,username:%s,视频:%s下载成功:%s,进度:%s/%s" % (
                myFtpRemote.host, myFtpRemote.username, video_full_path, video_local_path, index_num, total_num))
            mutex.release()
            break
        except Exception as e:
            myLog.warning(u"HOST:%s,username:%s,视频：%s,存储:%s,下载失败,%s秒后重试,进度:%s/%s" % (
                myFtpRemote.host, myFtpRemote.username, video_full_path, video_local_path,
                setting.WRONG_SLEEP ** retry_time, index_num, total_num))
            myLog.error(e)
            myFtpRemote.ftp = None
            time.sleep(setting.WRONG_SLEEP ** retry_time)
            retry_time += 1
        finally:
            fp.close()
    if result:
        down_size = os.stat(video_local_path).st_size - lsize_start
        mutex.acquire()
        down_size_dict["down_size"] = down_size_dict.get("down_size", 0) + down_size
        success_num = down_success.get("success", 0) + 1
        down_success["success"] = success_num
        mutex.release()
    else:
        mutex.acquire()
        myLog.warning(u"HOST:%s,username:%s,视频：%s,存储:%s,下载失败,进度:%s/%s" % (
            myFtpRemote.host, myFtpRemote.username, video_full_path, video_local_path, index_num, total_num))
        download_failed.info(u"HOST:%s,username:%s,视频：%s,存储:%s" % (
            myFtpRemote.host, myFtpRemote.username, video_full_path, video_local_path))
        failed_num = down_failed.get("failed", 0) + 1
        down_failed["failed"] = failed_num
        mutex.release()

    return result


def mrss_choice(data):
    """
    mrss源视频下载
    :param data:{}
    :return:None
    """
    down_link = data.get("down_link")
    video_url = data.get("file_path")
    file_size = data.get("file_size")

    #############下载##############
    res = mrss_download(video_url, down_link, file_size)
    if res:
        urllib_post(data)


def file_exists(video_url):
    retry_time = 1
    obj_exists = None
    global bucket_video
    while retry_time <= setting.RETRY_TIMES:
        try:
            if not bucket_video:
                bucket_video = get_bucket_video(video_url)
            obj_exists = bucket_video.object_exists(video_url)
            break
        except Exception as e:
            bucket_video = None
            obj_exists = None
            myLog.warning(u"bucket链接有误,%s秒后重试" % setting.WRONG_SLEEP ** retry_time)
            myLog.warning(e)
            time.sleep(setting.WRONG_SLEEP ** retry_time)
            retry_time += 1
    if obj_exists:
        mutex.acquire()
        exists_num = down_exists.get("exists") + 1
        down_exists["exists"] = exists_num
        myLog.info(u"oss对象%s已存在" % video_url)
        video_exists.info(u"oss对象%s已存在" % video_url)
        mutex.release()
    return obj_exists


def task_choice(data, source):
    """
    根据不同来源,分发不同的工作流
    :param data:
    :return:
    """
    if source == "mrss":
        mrss_choice(data)
    else:
        remote_choice(data)


def thread_status():
    """
    控制最大线程数量为
    :return:
    """
    while True:
        if len(threading.enumerate()) > setting.THREAD_MAX:
            time.sleep(5)
        else:
            return True


def check_threads():
    """
    等待所有线程完成
    :return:
    """
    while True:
        if len(threading.enumerate()) == 1:
            return True
        else:
            time.sleep(5)


if __name__ == "__main__":
    myLog.info(u"进程开启")
    redis_pool = RedisPool(setting.REDIS_HOST, setting.REDIS_PORT, setting.REDIS_DB)
    redis_client = redis_pool.redis_client()
    try:
        value = redis_client.get("download_status")
    except Exception as e:
        value = None
        myLog.warning(u"redis链接失败,程序退出")
        myLog.warning(e)
        sys.exit()
    if value != "free":
        myLog.warning(u'下载程序正在运行,无法再次启动')
        sys.exit()
    else:
        try:
            redis_client.set("download_status", "downloading", xx=True)
        except Exception as e:
            myLog.warning(u"redis链接失败,程序退出")
            myLog.warning(e)
            sys.exit()
    source_tuple = get_source_tuple()
    mrss_dict = source_tuple[0]
    remote_dict = source_tuple[1]
    source_list = mrss_dict.values() + remote_dict.values()
    thread_obj = None
    for source_dict in source_list:
        source = source_dict.get("source")
        down_info_list = get_down_info_list(source_dict, source)
        for down_data in down_info_list:
            if thread_status():
                thread_obj = threading.Thread(target=task_choice, args=(down_data, source))
                thread_obj.setDaemon(True)
                thread_obj.start()
    if thread_obj:
        thread_obj.join()
    if check_threads():
        myLog.info(u"下载进程结束,修改源状态")
        try:
            req = requests.post("%s%s" % (setting.SERVER, setting.UPDATE_STATUS),
                                data={"mrss_ids": mrss_dict.keys(), "remote_ids": remote_dict.keys()}, verify=False,
                                timeout=120)
            status_code = req.status_code
            if status_code != 200:
                myLog.warning(u"源状态修改失败")
            else:
                myLog.warning(u"源状态修改成功")
        except Exception as e:
            myLog.warning(u"源状态修改失败")
            myLog.error(e)
            video_index_dict = {"video_index": 0}
            video_total_dict = {"video_total": 0}
        total_seconds = down_time_dict.get("down_time", 0)
        total_size = down_size_dict.get("down_size", 0)
        myLog.info(
            u"视频总数:%s,下载成功视频数:%s" % (video_total_dict.get("video_total", 0), video_index_dict.get("video_index", 0)))
        myLog.info(u'下载视频总大小:%s' % return_format_size(total_size))
        myLog.info(u'下载视频总耗时:%s' % return_format_time(total_seconds))
        myLog.info(u'平均下载速度:%s Kb/s' % (total_size / total_seconds))
        try:
            redis_client.set("download_status", "free", xx=True)
        except:
            try:
                redis_client = redis_pool.redis_client()
                redis_client.set("download_status", "free", xx=True)
            except Exception as e:
                myLog.warning(u"redis状态修改失败")
                myLog.warning(e)
        myLog.warning(u"进程结束")
        sys.exit()
