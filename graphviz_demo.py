# from graphviz import Digraph
# dot = Digraph(comment='这是一个有向图')
# dot.node('A', '作者')
# dot.node('B', '医生')
# dot.node('C', '律师')

# dot.edges(['AB', 'AC'])
# dot.edge('B', 'C')
# print(dot.source)
# dot.render('output-graph.gv', view=True)
import re
import os
import sys
import pprint
from graphviz import Digraph

class graphivz_sql(object):
    def __init__(self, dir_name):
        # self.dir_name = dir_name
        self.dot_dir = os.path.join(dir_name,'dot')
        if not os.path.exists(self.dot_dir):
            os.makedirs(self.dot_dir)
        self.get_all_dot_file(dir_name)
        self.merge_get_png()
        # self.get_dot(self)
        # self.get_png()

    def get_dot(self, target_file):
        # r"D:\code\test\daily_code\ceshi.py"
        with open(target_file, 'r', encoding='UTF-8') as f:
            content = "".join(f.readlines()) 
            sources = []
            # sources.extend([item for item in re.findall(r'^(?!.*extract).*from (\S*)[ |\n]', content, re.M)])
            sources.extend([i for item in re.findall(r'^(?!.*(extract|import)).*from\s*(\S*)[ |\n]', content, re.M) \
                 for i in item if not i.startswith('tmp_') and i!='' and i!='source_table' and 'unix' not in i])
            sources.extend([item for item in re.findall(r'.*join\s*(\S*)[ |\n]', content, re.M) if not item.startswith('tmp_')])
            # print(sources)
            sources = list(set(sources))
            target = list(set([i for item in re.findall(r'.*(into|overwrite)\s*table\s*(\S*)[ |\n]', content, re.M) for i in item if i not in ('into','overwrite')]))
            # print(target)
        # print(sources)    
        # print(target)
        if target==[]:
            print("这个脚本没有明确的目标表")
            return
        # print(target)
        dirname, filename = os.path.split(target_file)
        file_name = filename.split('.')[0]
        dot1 = ["digraph g {", "}"]
        # dot = Digraph(comment='The Test Table')
        for item in sources:
            dot1.insert(1,'"{0}" -> "{1}"'.format(item, target[0]))#'"'+item+'"', '"'+target[0]+'"'
            # dot.edge(item, target[0])
        with open(os.path.join(self.dot_dir,file_name+'.dot'), 'w') as f:
            f.write("\n".join(dot1))
        # os.system('dot {0} -T png -o {0}.png'.format(os.path.join(self.dot_dir,file_name+'.dot')))
        # dot.format = 'svg'
        # 保存source到文件，并提供Graphviz引擎
        # dot.render()  
    def get_all_dot_file(self, dir):
        for path, dir_list, file_list in os.walk(dir):
            for file_name in file_list: #遍历文件
                # print(file_name)
                if file_name.endswith('.py'):
                    print(os.path.join(path, file_name))
                    # print(1)
                    self.get_dot(os.path.join(path, file_name))
            for dir_name in dir_list:
                # print(dir_name)
                if dir_name!='dot':
                    print(os.path.join(dir, dir_name))
                    # print(2)
                    self.get_all_dot_file(os.path.join(dir, dir_name))

    def merge_get_png(self,):
        # dir = r"D:\code\test\daily_code\dot"
        dot = [r"digraph g { rankdir=LR ", "}"]
        base_dot = [r"digraph g { rankdir=LR ", "}"]
        al = []
        words = []
        target = {'ods':[],'edw':[], 'edw_ai':[], 'dm':[],'rst':[],'oth':[]}
        for file in os.listdir(self.dot_dir):
            if file != 'res.dot' and file.endswith('.dot'):
                with open(os.path.join(self.dot_dir, file),'r') as f:
                    # print(file)
                    # al.extend([line for line in f.readlines()[1:-1]])
                    for line in f.readlines():
                        # print(line)
                        if '->' in line:
                            # al.extend(line)
                            base_dot.insert(1, line)
                            # left, right = [item.strip() for item in line.split('->')]
                            for item in [item.strip() for item in line.split('->')]:
                                if 'edw' in item and 'edw_ai' not in item:
                                    target['edw'].append(item)
                                elif 'edw_ai' in item:
                                    target['edw_ai'].append(item)
                                elif 'ods' in item:
                                    target['ods'].append(item)
                                elif 'dm' in item:
                                    target['dm'].append(item)
                                elif 'rst' in item:
                                    target['rst'].append(item)
                                else:
                                    target['oth'].append(item)
                                # for key in target.keys():
                                #     if key in item:
                                #         target[key].append(item)
                                #         cnt += 1
                                # if not cnt:
                                #     target['oth'].append(line)
                    # words.extend([item.strip() for item in line.split('->')])
                            # dot.insert(1, line)
        # dot.insert(1, *al)
        # 分类
        
        # for item in words:
        #     cnt = 0
        #     for key in target.keys():
        #         if key in item:
        #             target[key].append(item)
        #             cnt += 1
        #     if not cnt:
        #         target['oth'].append(line)
            # pprint.pprint(list(set(words)))
        # pprint.pprint(target)
        # base_dot = [r"digraph g { rankdir=LR ", "}"]
        # dot = [r"subgraph '{0}' { rankdir=LR ", "}"]
        color = {'ods':'pink', 'edw':'yellow', 'edw_ai':'green', 'dm':'purple', 'rst':'beige', 'oth':'red'}
        # bgcolor="pink"
        for key, value in target.items():
            d = ["subgraph cluster_{0} ".format(key), '{ rankdir=LR', 'bgcolor="{0}"'.format(color[key]), "}"]
            # d.insert(2,color[key])
            for item in value:
                d.insert(3,item)
            # base_dot.insert(1, d)
            # print("\n".join(d))
            base_dot.insert(1, "\n".join(d))
        # d = "\n".join(base_dot)
        pprint.pprint(base_dot)
        # for item in al:
        #     base_dot.insert(1, al)
        # print("\n".join(base_dot))
        with open(os.path.join(self.dot_dir, 'res.dot'), 'w') as f:
            f.write("\n".join(base_dot))
        os.system('dot {0} -T png -o {0}.png'.format(os.path.join(self.dot_dir, 'res.dot')))
        # os.system('dot {0}.dot -T png -o {0}.png'.format(self.file))
        # dot1 = Digraph(comment='The Test Table')
        # for line in dot[1:-1]:
        #     left, right= line.split('->')
        #     dot1.edge(left, right)
        # dot1.format = 'svg'
        # # 保存source到文件，并提供Graphviz引擎
        # dot1.render()  
        # print("保存 成功")
        # dot.render('test-table.gv', view=True)

if __name__ == "__main__":
    file = r"""D:\code\test\daily_code\ceshi.py"""
    dir_name = """D:\gongsi_project\product\linezone_retail_wh_1\Hadoop\spark_code"""
    # dir_name = sys.argv[1]
    obj = graphivz_sql(dir_name)





