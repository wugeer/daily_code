"""
description:平时工作中需要用到的简单时间函数汇总
author:xiehognwang 
last_modified:2019-10-17
version:0.1
"""
import datetime
import time


def get_current_day():
    """
    获取当前日期
    """
    # strftime将时间格式化为我们想要的时间字符串，常见的%Y-%m-%d，那么我们会得到2019-10-12这样的数据
    print(time.strftime('%Y-%m-%d'))
    print(datetime.datetime.now().strftime('%Y-%m-%d'))


def get_currnet_time():
    """
    获取当前时间戳
    """
    # 这个得到的是只精确到秒而已
    print(time.strftime('%Y-%m-%d %H:%M:%S'))
    # 而这个是精确到毫秒的
    print(datetime.datetime.now())


def get_next_time_interval(days=0,
                           seconds=0,
                           microseconds=0,
                           milliseconds=0,
                           minutes=0,
                           hours=0,
                           weeks=0,
                           tm_format='%Y-%m-%d %H:%M:%S'):
    """
    得到当前时间戳间隔给定给定时间范围的时间字符串,参数都是可选的并且默认为 0。这些参数可以是整数或者浮点数，也可以是正数或者负数。
    :param days: 天数
    :param seconds: 秒数
    :param microseconds: 毫秒数
    :param milliseconds: 微秒数
    :param minutes: 分钟数
    :param hours: =小时数
    :param weeks: 周数
    :return 时间戳或者时间
    """
    next_interval = datetime.datetime.now() + datetime.timedelta(
        days=days,
        seconds=seconds,
        microseconds=microseconds,
        milliseconds=milliseconds,
        minutes=minutes,
        hours=hours,
        weeks=weeks)
    # print(next_interval.strftime('%Y-%m-%d'))
    return next_interval.strftime(tm_format)


def get_time_year_month_day(that_day, tm_format='%Y-%m-%d'):
    # print(type(that_day))
    """
    获取给定时间字符串对应的年月日
    :param that_day: 时间字符串
    :param tm_format:和that_day相匹配的时间格式字符串，常见的为"%Y-%m-%d %H:%M:%S"和"%Y-%m-%d",默认是后者
    :return 给定时间字符串对应的年月日依次返回，用逗号分开，比如2019,10,10
    """
    if not isinstance(that_day, datetime.datetime):
        that_day = datetime.datetime.strptime(that_day, tm_format)
    print(that_day.year, that_day.month, that_day.day)
    return that_day.year, that_day.month, that_day.day


def TimeToTimeSpan(tm_info, tm_fomat="%Y-%m-%d %H:%M:%S"):
    """
    传入一个和tm_fomat匹配的时间字符串，然后计算和该时间和1970-01-01 08:00:00相差的秒数(浮点型)，并返回，该差值不能为负数，否则报错.
    如果是时间诸如2019-01-01那么默认是2019-01-01 00:00:00;传入的时间不允许包含毫秒等秒下一级别的数据
    :param tm_info: 时间字符串
    :param tm_format:和tm_info相匹配的时间格式字符串，常见的为"%Y-%m-%d %H:%M:%S"和"%Y-%m-%d"
    :return 整型的传入时间和1970-01-01 08:00:00相差的秒数
    """
    return time.mktime(time.strptime(tm_info, tm_fomat))


def TimeStampToTime(timestamp):
    """
    把时间差转为对应的时间戳，基于1970-01-01 08:00:00
    """
    # 第一种方法使用time模块
    # 第二种方法使用datetime模块
    # print(datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S"))
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))


def GetWeekDay(that_day, tm_format='%Y-%m-%d'):
    """
    传入一个时间字符串和该字符串相匹配的格式字符串，获取该天对应的星期
    :param that_day: 时间字符串
    :param tm_format:和that_day相匹配的时间格式字符串，常见的为"%Y-%m-%d %H:%M:%S"和"%Y-%m-%d",默认是后者
    :return 该天的星期
    """
    week_day_dict = {
        0: '星期一',
        1: '星期二',
        2: '星期三',
        3: '星期四',
        4: '星期五',
        5: '星期六',
        6: '星期天'
    }
    if not isinstance(that_day, datetime.datetime):
        that_day = datetime.datetime.strptime(that_day, tm_format)
    return week_day_dict[that_day.weekday()]


if __name__ == "__main__":
    # 	print(datetime.datetime.now())
    # 	print(TimeToTimeSpan('1970-01-02', '%Y-%m-%d'))
    # 	print(TimeToTimeSpan('1970-01-02 00:00:00'))
    print(get_time_year_month_day('2019-10-17'))
#     print(TimeStampToTime(0))