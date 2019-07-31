import re 
import pprint
import os
from collections import Counter

class join_cnt(object):
    def __init__(self, dir_name):
        self.dir_name = dir_name
        # 如果目标文件存在则删除，这是因为采用的追加模式
        # if os.path.exists(os.path.join(self.dir_name, 'res.txt')):
        #     os.remove(os.path.join(self.dir_name, 'res.txt'))
        self.res_dir = os.path.join(dir_name,'res')
        # 建立一个dot文件存放每个子文件的dot文件和最终的dot文件，先判断是否存在，不存在则创建
        if not os.path.exists(self.res_dir):
            os.makedirs(self.res_dir)
        self.get_all_join_file(self.dir_name)
        self.merge_file()
    def get_join_number(self, target_file):
        with open(target_file, 'r', encoding='UTF-8') as f:
            content = " ".join([item.rstrip() for item in f.readlines()])
        # 获取所有的SQL段
        con = re.findall(r'"""(.*?)"""',content,re.DOTALL|re.M)[1:]
        temp = []
        for item in con:
            join_count = len(re.findall(r'join',item, re.M))
            from_count = re.findall(r'from',item, re.M)
            # print(join_count)
            tm = [join_count+1]
            # temp.extend(join_count+1)
            list_group_by = re.findall(r'group by(.*?)(?=order by|limit|having|\)|;)',item, re.M|re.DOTALL)
            if list_group_by:
                tm.extend([len(item.split(',')) for item in list_group_by])
            else:
                tm.append(0)
            temp.append(tm)
        if not temp:
            print("无关表{0}".format(target_file))
            return
        # pprint.pprint(temp)
        print("开始写入")
        dirname, filename = os.path.split(target_file)
        with open(os.path.join(self.res_dir, '{0}.txt'.format(filename)), 'w') as f: #os.path.join(self.dir_name, 'res.txt')
            f.write("file_name\t次序\tjoin个数\tgroup by字段个数\n")
            for idx,value in enumerate(temp):
                # print('key:{0}, value:{1}'.format(key,value))
                f.write("{0}\t{1}\t{2}\t{3}\n".format(filename, idx+1, *value))
            
        # pprint.pprint(d)
    def get_all_join_file(self, dir):
        """
        生成整个工程的dot文件
        """
        for path, dir_list, file_list in os.walk(dir):
            for file_name in file_list: #遍历文件
                if file_name.endswith('.py'):
                    print(os.path.join(path, file_name))
                    self.get_join_number(os.path.join(path, file_name))
            for dir_name in dir_list:
                # if dir_name!='dot':
                print(os.path.join(dir, dir_name))
                self.get_all_join_file(os.path.join(dir, dir_name))

    def merge_file(self,):
        join_dict, group_dict = {},{}
        for file_name in os.listdir(self.res_dir):
            if file_name.endswith('.txt') and file_name != 'res_join.txt':
                # print(os.path.join(self.res_dir,file_name))
                with open(os.path.join(self.res_dir,file_name), 'r')as f:
                    join_count_list, group_by_count_list = [],[]
                    for idx,line in enumerate(f.readlines()):
                        # 踢掉第一行
                        if idx:
                            file,idx,join_cnt, group_cnt = line.split('\t')
                            # 根据文件中的每一行，确定join个数为n的文件列表，group by的类似
                            if join_cnt in join_dict:
                                join_dict[join_cnt].append(file)
                            else:
                                join_dict[join_cnt] = [file]
                            if group_cnt in group_dict:
                                group_dict[group_cnt].append(file)
                            else:
                                group_dict[group_cnt] = [file]
                        # join_count_list.append(join_cnt)
                        # group_by_count_list.append(group_cnt)
        # pprint.pprint(join_dict)
        pprint.pprint(group_dict)
        with open(os.path.join(self.res_dir, 'res_join.md'), 'w', encoding='UTF-8') as f:
            row = join_dict.keys()
            out_col = set()
            # out_col.update(["次数"])
            out_col.update([i for item in join_dict.values() for i in item])
            # out_col.update(["\n"])
            # print(out_col)
            first_line = "|次数|"+"|".join(out_col)+"|"
            # pprint.pprint(first_line)
            # print("\n".join([str(i) for i in d]))
            f.write(first_line)
            f.write('\n')
            sep = ['---']*len(out_col)
            f.write("|---|"+"|".join(sep)+"|")
            f.write('\n')
            for key,value in join_dict.items():
                d = Counter(value)
                line = [key,]
                for col in out_col:
                    if col in d:
                        line.append(d[col])
                    else:
                        line.append(0)
                # line.append('\n')
                f.write("|"+"|".join([str(i) for i in line])+"|")
                f.write('\n')
        with open(os.path.join(self.res_dir, 'res_group.md'), 'w', encoding='UTF-8') as f:
            row = group_dict.keys()
            out_col = set()
            # out_col.update(["次数"])
            out_col.update([i for item in group_dict.values() for i in item])
            # out_col.update(["\n"])
            # print(out_col)
            first_line = "|次数|"+"|".join(out_col)+"|"
            # pprint.pprint(first_line)
            # print("\n".join([str(i) for i in d]))
            f.write(first_line)
            f.write('\n')
            sep = ['---']*len(out_col)
            f.write("|---|"+"|".join(sep)+"|")
            f.write('\n')
            idx = 0
            for key,value in group_dict.items():
                d = Counter(value)
                line = [key.strip(),]
                print(idx)
                idx += 1
                # if int(key.strip())==0:
                #     print(out_col)
                for col in out_col:
                    if col in d:
                        line.append(d[col])
                    else:
                        line.append(0)
                # line.append('\n')
                f.write("|"+"|".join([str(i) for i in line])+"|")
                # f.write('{0}\n'.format(idx))
                f.write('\n')



if __name__ == "__main__":
    # target_file = r
    dir_name = """D:\gongsi_project\product\linezone_retail_wh_1\Hadoop\spark_code"""
    # file_name = r"""D:\code\test\daily_code\p_dim_source_bom.py"""
    obj = join_cnt(dir_name)
    # obj.get_join_number(file_name)