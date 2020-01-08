import logging.config
import logging

from usual_decorator import singleton


@singleton
class my_logger(object):
    def __init__(self,):
        pass

    def get_logger(self, config_file_name, logger_name="sampleLogger"):
        """
        根据日志器的配置文件和相应的日志器获取相应的日志器
        :param config_file_name: 日志器的配置文件路径
        :param logger_name: 日志器名称
        """
        print(config_file_name)
        logging.config.fileConfig(fname=config_file_name)  # , disable_existing_loggers=False
        logger = logging.getLogger(logger_name)
        return logger


def test(obj):
    obj.debug("这是一条debug信息")
    obj.info("这是一条info信息")
    obj.warning("这是一条warning信息")
    obj.error("这是一条error信息")
    raise Exception("测试抛出异常")


if __name__ == "__main__":
    # pass
    # test()
    config_file_name = "test.ini"
    obj1 = my_logger().get_logger(config_file_name)
    obj2 = my_logger().get_logger(config_file_name, 'root')
    # if obj1 is obj2:
    print(" obj1 is obj2: {}".format(obj1 is obj2))
    obj1.debug("这是一条debug信息")
    obj2.info("这是一条info信息")
    obj1.warning("这是一条warning信息")
    obj2.error("这是一条error信息")
    try:
        1/0
    except ZeroDivisionError as e:
        obj1.exception("错误信息如下: {}".format(str(e)))
    finally:
        test(obj1)
