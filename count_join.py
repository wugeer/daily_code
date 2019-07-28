import re 
import pprint
import os

class join_cnt(object):
    def __init__(self, dir_name):
        self.dir_name = dir_name
        self.get_all_join_file(self.dir_name)
    def get_join_number(self, target_file):
        with open(target_file, 'r', encoding='UTF-8') as f:
            content = " ".join([item.rstrip() for item in f.readlines() if item.strip() and 'drop' not in item and 'delete' not in item])
        # 获取所有的SQL段
        temp = [i for item in re.findall(r'(create|insert)?([^;]*);', content, re.M|re.DOTALL) for i in item if len(i)>6]
        # print(temp)
        d = {}
        for s in temp:
            # 每个SQL段的表名
            key = [i for item in re.findall(r'(table|into)\s*([a-zA-Z._0-9]*)', s)for i in item if i not in ('table', 'into')]
            if not key:
                continue
            key = key[0]
            # d.update(name=key)
            # print([i for item in re.findall(r'(table|into)\s*([a-zA-Z._0-9]*)', s) for i in item if i not in ('table', 'into')])
            tm = {}
            # SQL段中join的个数
            tm['join_cnt']=len(re.findall(r'join',s, re.M))+1
            # SQL段中group by字段个数，注意此处认定如果同时出现group by和order by 则认为order by在后面
            sub = [item for item in re.findall(r'group\s*by\s*(.*)',s, re.M|re.DOTALL)]
            if sub:
                for it in sub:
                    if 'order' in it:
                        tm['group_by_cnt']=len(re.match(r'(.*)order', it, re.M|re.DOTALL).group()[0].split(','))
                        # print(len(re.match(r'(.*)order', it, re.M|re.DOTALL).group()[0].split(',')))
                        continue
                    # print(len(it.split(',')))
                    tm['group_by_cnt']=len(it.split(','))
            else:
                tm['group_by_cnt']=0 
            # 每个SQL段作为一个字段传入总的字典中
            d[key] = tm
            with open(os.path.join(r'd:', 'res.txt'), 'a') as f: #os.path.join(self.dir_name, 'res.txt')
                for key,value in d.items():
                    f.write("{0}\t{1}\t{2}".format(target_file, key, value))
        
        # pprint.pprint(d)
    def get_all_join_file(self, dir):
        """
        生成整个工程的dot文件
        """
        for path, dir_list, file_list in os.walk(dir):
            for file_name in file_list: #遍历文件
                if file_name.endswith('.sql'):
                    print(os.path.join(path, file_name))
                    self.get_join_number(os.path.join(path, file_name))
            for dir_name in dir_list:
                # if dir_name!='dot':
                print(os.path.join(dir, dir_name))
                self.get_all_join_file(os.path.join(dir, dir_name))

if __name__ == "__main__":
    # target_file = r"""D:\code\test\daily_code\ceshi.py"""
    # get_join_number(target_file)
    dir_name = """D:\gongsi_project\product\linezone_retail_wh_1\Hadoop\spark_code"""
    obj = join_cnt(dir_name)