""" 
author:xiehongwang
last_modified:2019-07-30 
description:通过传入工程的路径，依次分析工程下的pyspark脚本，生成每个脚本的dot文件，并生成最终的dot文件和png图片,剔除临时表,只保留物理表
version:v1_1
note：传入pyspark脚本中SQL串需遵守一定的规则，否则难以确定匹配规则，比如from某一个表需要另起一行
"""
import re
import os
import sys
from graphviz import Digraph
import pprint

class graphivz_dag(object):
    """
    分析整个工程下的pyspark脚本，每个脚本生成一个dot文件，并依此生成最终的dot文件和png图片
    """
    def __init__(self, dir_name):
        # self.dir_name = dir_name
        self.dot_dir = os.path.join(dir_name,'dot')
        # 建立一个dot文件存放每个子文件的dot文件和最终的dot文件，先判断是否存在，不存在则创建
        if not os.path.exists(self.dot_dir):
            os.makedirs(self.dot_dir)
        # 生成目录下所有pyspark文件对应的dot文件
        self.get_all_dot_file(dir_name)
        # 根据上面生成的dot文件，合并成最终的dot文件，并使用dot命令生成png图片
        self.merge_get_png()

    def get_dot(self, target_file):
        """ 
        获取Python脚本里面的目标表和涉及的原表，剔除临时表
        """
        try:
            with open(target_file, 'r', encoding='UTF-8') as f:
                # 获取脚本里面的内容，小文件就不考虑内存爆炸
                # 对于空格，可以考虑遍历每一个行时，将\t，\n等换成一个空格隔开，然后拼接成一个字符串content,这个后面再优化
                content = "".join(f.readlines()) 
                # 原表
                sources_table = []
                # 排除含有extra，import，Unix等子串的行，获取from后面的原表，删除临时表，source_table这个属于特例，某个文件中用到了这个
                sources_table.extend([i for item in\
                     re.findall(r'^(?!.*(extract|import|unix)).*from\s*(\S*)[ |\n]', content, re.M) \
                        for i in item if not i.startswith('tmp_') and i!='' and i!='source_table']) # 
                # 获取join后面的原表，删除临时表
                sources_table.extend([item for item in re.findall(r'.*join\s*(\S*)[ |\n]', content, re.M) if not item.startswith('tmp_')])
                # 获取目标表并去重
                target = list(set([i for item in re.findall(r'.*(into|overwrite)\s*table\s*(\S*)[ |\n]', content, re.M) for i in item if i not in ('into','overwrite')]))
                if target==[]:
                    print("这个脚本没有明确的目标表")
                    return
                dirname, filename = os.path.split(target_file)
                # 这里认定文件名的格式为file_name.filetype，即文件名中出现数字字母和下划线
                file_name = filename.split('.')[0]
                # dot模板
                dot1 = ["digraph g {",]
                # 拼接边对应的字符串
                dot1.extend(['"{0}" -> "{1}"'.format(item, target[0]) for item in sources_table if item!=target[0]])
                dot1.append( "}")
                # for item in sources_table:
                #     # 拼接dot文件内容,剔除临时表和自身调用
                #     if item==target[0]: #and 'ods_standard' not in item or ('ods' in item )    or ('ods' in item )
                #         continue
                #     dot1.insert(1,'"{0}.py" -> "{1}.{2}"'.format(item, target[0].split('.')[0], filename))#  '"'+item+'"', '"'+target[0]+'"'
                with open(os.path.join(self.dot_dir,file_name+'.dot'), 'w', encoding='UTF-8') as f:
                    f.write("\n".join(dot1))
                #生成每个文件对应dag图对应的png
                os.system('dot {0} -T png -o {0}.png'.format(os.path.join(self.dot_dir,file_name+'.dot')))
        except FileNotFoundError as e:
            print('指定的文件无法打开.{0}'.format(e))
        except LookupError as e:
            print('指定了未知的编码!.{0}'.format(e))
        except UnicodeDecodeError as e:
            print('读取文件时解码错误!.{0}'.format(e))
        except IOError as e:
            print('读写文件时出现错误.')
        finally:
            print("生成{0}脚本的dot文件".format(target_file))
           
    def get_all_dot_file(self, dir):
        """
        生成整个工程的dot文件
        """
        for path, dir_list, file_list in os.walk(dir):
            # 遍历文件
            for file_name in file_list: 
                if file_name.endswith('.py'):
                    print(os.path.join(path, file_name))
                    self.get_dot(os.path.join(path, file_name))
            # 文件夹列表
            for dir_name in dir_list:
                if dir_name!='dot':
                    print(os.path.join(dir, dir_name))
                    self.get_all_dot_file(os.path.join(dir, dir_name))

    def merge_get_png(self,):
        """
        合并每个脚本的dot文件节点内容，分层，生成png图片
        """
        base_dot = ["digraph g { rankdir=LR;ranksep=10;node [ style = invis ];", "}"]
        target = {'ods':[],'edw':[], 'edw_ai':[], 'dm':[],'rst':[],'oth':[]}
        line_list = set()
        for file in os.listdir(self.dot_dir):
            if file != 'res.dot' and file.endswith('.dot'):# 跳过最终的dot文件
                try:
                    with open(os.path.join(self.dot_dir, file),'r') as f:
                        for line in f.readlines():
                            if '->' in line and line not in line_list:
                                line_list.add(line)
                                base_dot.insert(1, line)
                                for item in [item.strip() for item in line.split('->')]:
                                    # 给每一个元素分层
                                    if 'edw' in item and 'edw_ai' not in item:
                                        if item not in target['edw']:
                                            target['edw'].append(item)
                                    elif 'edw_ai' in item:
                                        if item not in target['edw_ai']:
                                            target['edw_ai'].append(item)
                                    elif 'ods' in item:
                                        if item not in target['ods']:
                                            target['ods'].append(item)
                                    elif 'dm' in item:
                                        if item not in target['dm']:
                                            target['dm'].append(item)
                                    elif 'rst' in item:
                                        if item not in target['rst']:
                                            target['rst'].append(item)
                                    else:
                                        if item not in target['oth']:
                                            target['oth'].append(item)
                except FileNotFoundError as e:
                    print('指定的文件无法打开.')
                except LookupError:
                    print('指定了未知的编码!')
                except UnicodeDecodeError:
                    print('读取文件时解码错误!')
                except IOError as e:
                    print('读写文件时出现错误.')
                finally:
                    print("拼接最终dot文件和表分类操作完成之一")
        # 给各层分配各颜色
        color = {'ods':'pink', 'edw':'yellow', 'edw_ai':'green', 'dm':'purple', 'rst':'beige', 'oth':'red'}
        # tt = ['ods':{},'edw':{}, 'edw_ai':{}, 'dm':{},'rst':{}]
        key_ = ['rst','dm','edw_ai','edw','ods','oth']
        # 按层次插入文件，控制生成的dag图中各层次的布局
        for index,class_value in enumerate(key_):
        # for key, value in target.items():
            # 生成子图,想给每层的元素做个排序，使得在dag中节点有顺序，但是无效
            key, value = class_value,target[class_value]
            # d = ["subgraph cluster_{0} ".format(key), '{ node [style=filled];color=blue;rankdir=LR;shape = box;','rank=same', 'bgcolor="{0}";'.format(color[key]), "}"]
            # 给每个后面的节点一个颜色
            # pprint.pprint(value)
            rank = ['node[shape = record,style=filled,width=5;color={0}]'.format(color[key]),';{rankdir=LR; rank=same;edge[constraint=false];',]
            rank.extend([item+"""[group=left][pos="{0},-{1}!"];\n""".format(idx,index) for idx,item in enumerate(value)])
            rank.append('}')
            # for idx,item in enumerate(value):
            #     # d.insert(4,item+';')
            #     rank.insert(2,item+"""[group=left][pos="{0},-{1}!"];\n""".format(idx,index))
            # base_dot.insert(1, "\n".join(d))
            base_dot.insert(1, "\n".join(rank))
        try:
            with open(os.path.join(self.dot_dir, 'res.dot'), 'w') as f:
                # 写入最终的dot文件
                f.write("\n".join(base_dot))
            # 生成png图片
            print("开始生成目标png")
            os.system('dot {0} -T png -o {0}.png'.format(os.path.join(self.dot_dir, 'res.dot')))
            print("生成目标png成功")
        except FileNotFoundError as e:
            print('指定的文件无法打开.')
        except LookupError:
            print('指定了未知的编码!')
        except IOError as e:
            print('读写文件时出现错误.')
        except Exception as e:
            print("出现了其他异常请查看{ex}".format(ex=e))
        finally:
            print("已经生成最终的png图片了")
        

if __name__ == "__main__":
    # 要分析的目录
    dir_name = r"""D:\gongsi_project\product\linezone_retail_wh_1\Hadoop\spark_code\edw"""
    # file_name = r"""D:\code\test\daily_code\test.py"""
    obj = graphivz_dag(dir_name)
    # obj.get_dot(file_name)





