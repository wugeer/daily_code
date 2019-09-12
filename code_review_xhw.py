""" 
description:自动化工具，包含文本处理类和画图类
author: xhw
last_modified:2019-08-21 
version:v_1_0
created:2019-08-21
modified:xiehongwang 2019-08-27 将原先的获取源表和目标表的一个函数拆成两个
"""
import re 
import xlrd
import os
import sys
import pprint
# from collections import Counter

class content_process(object):
    """
    文本处理类，含获取脚本中的源表和目标表，group by字段个数，join连接个数，ddl中表及其字段，Excel中表及其字段，py脚本目标表及其字段
    """
    def __init__(self, ):
        pass 

    def get_ddl_dict(self, file_name):
        """
        获取ddl中表名和字段
        输入:文件路径
        输出：字典，键值是表名，值是字段的列表
        """
        content = self.get_re_sql_content(file_name)
        # pprint.pprint(content)
        res_dict = {}
        for first,second in re.findall(r'(?<!drop) table ([a-zA-Z0-9._]+)\s?(.*?);', content, re.I|re.DOTALL):
            # print(first+'\n'+second)
            # print(second)
            temp_key = re.findall(r'[(](.*?)[)]', second)
            # print(temp_key)
            res_dict[first.split('.')[-1]] = {i.strip().split(' ')[0] for item in temp_key \
                 for i in item.split(',') if i.strip() and re.match(r'[a-z0-9A-Z_]+$', i.strip().split(' ')[0])}
        # pprint.pprint(res_dict)
        return res_dict

    def get_excel_dict(self, file_name):
        """
        此处默认正常的sheet页的名字是有下划线的
        获取Excel数据字典中每个表和字段
        输入:Excel文件绝对路径
        输出:字典，键值是表名，值是字段的列表
        """
        # 打开文件
        wb = xlrd.open_workbook(filename=file_name)
        # 获取所有sheet页名字
        sheet_names = wb.sheet_names()
        # print(sheet_names)
        res_dict = {}
        for index in sheet_names:
            if '_' not in index:
                continue
            sheet =  wb.sheet_by_name(index)
            # print(index)
            # for col in range(sheet.ncols):
            try:
                res_dict[index] = set([re.findall(r'Column,(.*)', ",".join([str(item) for item in sheet.col_values(col) if item]), re.I)[0] \
                for col in range(sheet.ncols) \
                if re.findall(r'Column,(.*)', ",".join([str(item) for item in sheet.col_values(col) if item]), re.I)][0].split(','))
                # print(res_dict[index])
            except IndexError as e:
                print(f"sheet页{index}有问题，报了{e}这个错误")
                continue
            # res_dict[str(index)] = {item for item in sheet.col_values(1) if item and item.lower()!='column'}
        return res_dict

    def get_re_py_content(self, file_name):
        """
        功能：对指定文件的内容进行格式化，仅对读出来的内容进行格式化，不改变源文件
        入参：file_name,py文件绝对路径
        返回值：格式化后的文本内容，str
        """
        with open(file_name, 'r', encoding='UTF-8') as f:
            # 剔除注释行和格式化多余空格为一个空格
            flag = False
            content = []
            for line in f.readlines():
                # 从if __name__程序入口开始获取内容
                if "if __name__" in line:
                    flag = True
                    continue
                # 删除注释行,统一为小写
                clean_line = re.sub(r'((--|#).*)','', line)
                if flag and clean_line.strip():
                    content.append(clean_line.lower())
            # content = re.sub(r'\s+', ' ', "".join([line for line in f.readlines() if re.sub('(--|#).*$','', line).strip()]))
            # 剔除多余空格
            content = re.sub(r'\s+', ' ', "".join(content))
            # 单引号改为双引号
            content = re.sub(r"'''", '"""', content)
        return content

    def get_re_sql_content(self, file_name):
        """
        功能：对指定文件的内容进行格式化，仅对读出来的内容进行格式化，不改变源文件
        入参： file_name, SQL文件绝对路径
        返回值：格式化后的文本内容，str
        """
        with open(file_name, 'r', encoding='UTF-8') as f:
            # 剔除注释行和格式化多余空格为一个空格
            content = re.sub(r'\s+', ' ', "".join([re.sub(r'((--|#).*)','', line) for line in f.readlines() \
                 if re.sub(r'((--|#).*)','', line).strip()])) # and 'drop table' not in line
            # 将诸如DECIMAL(38,10)中的(38,10)去掉
            content = re.sub(r'\([\d,]*\)','', content)
        return content

    def get_columns_of_table(self, content):
        cnt_left = 0
        tt = []
        for item in content:
            if item==')':
                cnt_left -= 1
            elif item=='(':
                cnt_left += 1
            elif not cnt_left:
                tt.append(item)
        return "".join(tt)
    
    def get_py_dict(self, file_name):
        """
        获取单个py文件的目标表及其字段
        传入参数：file_name，py文件的绝对路径
        返回值：字典，key是表名，value是字段集合
        """
        content = self.get_re_py_content(file_name)
        # print(content)
        # 这个是由于有些脚本是直接插入指定数据的比如一天24小时的，所以跳过这样的
        if re.search(r'values', content, re.I):
            return
        res_dict = {}
        code_segment = [line for item in re.findall(r'"""(.*?)"""', content) \
            for line in re.findall(r' table ([{}a-zA-Z._0-9]+) (?=partition|).*?select(.*?)from ', item, re.I) \
                if line]
        for table_name, values in code_segment:
            # print(re.sub(r'[(].*?[)]',' ', values).split(','))
            # 考虑到里面有多层嵌套循环，需要剔除里面的干扰
            item = self.get_columns_of_table(values)
            # pprint.pprint(f"table_name:{table_name}\nvalues:{values}")
            # print(re.sub(r'\(.*?\)','', re.sub(r'\(.*?\)','', values)))
            res_dict[table_name.split('.')[-1]] = {entry.strip().split(' ')[-1].split('.')[-1] for entry in item.split(',')} #item.strip().split(' ')[-1]
        return res_dict
    
    def get_py_dir_dict(self, dir_name):
        """
        传入参数：dir_name，py文件夹的绝对路径
        返回：字典，key是表名，value是字段集合
        功能：获取文本夹所有py文件目标表及其字段
        """
        dir_dict = {}
        with os.scandir(dir_name) as it:
            for entry in it:
                if entry.is_file() and entry.name.endswith('.py'):
                    # print(entry.name)
                    temp_dict = self.get_py_dict(os.path.join(dir_name, entry.name))
                    # print(f"temp_dict:{temp_dict}")
                    if temp_dict:
                        dir_dict.update(temp_dict)
        return dir_dict

    def get_origin_table(self, file_name):
        """
        传入参数：file_name，文件的绝对路径
        返回：源表的列表形式，举个例子原表为b,c,d表，那么返回的是return [b,c,d]
        功能：获取文本中的源表
        """
        content = self.get_re_py_content(file_name)
        # 排除含有extra，import，Unix等子串的行，获取from后面的原表，删除临时表，source_table这个属于特例，某个文件中用到了这个
        sources_table = re.findall(r"(?<!extract) from ([{}\w]+\.\w+) (?!import)", content, re.I)
        # 获取join后面的原表，删除临时表
        sources_table.extend(re.findall(r' join ([{}\w]+\.\w+)', content, re.I))
        # target_table = list((item[1] for item in re.findall(r'((?<=into)|(?<=overwrite)) table ([{}\w]+\.\w+)', content, re.I)))
        return sources_table

    def get_target_table(self, file_name):
        """
        传入参数：file_name，文件的绝对路径
        返回：目标表，举个例子目标表为a表，那么返回的是return [a]
        功能：获取文本中的目标表
        """
        content = self.get_re_py_content(file_name)
        # 获取目标表并去重
        target_table = re.findall(r'(?<=into) table ([{}\w]+\.\w+)', content, re.I)
        target_table.extend(re.findall(r'(?<=overwrite) table ([{}\w]+\.\w+)', content, re.I))
        return target_table

    def get_join_count(self, file_name): 
        """
        传入参数：file_name，文件的绝对路径
        返回：以字典的形式返回每一段中join和from个数之和
        功能：文本中以Python三个双引号内的内容为每一段，剔除第一个（默认是脚本的一些说明），其余的计算每个段中join个数据+1，即可
        """
        content = self.get_re_py_content(file_name)
        # print(content)
        code_segment = re.findall(r'"""(.*?)"""', content)
        target_list = []
        for seg in code_segment:
            # 拆分union
            for item in re.split('union', seg, maxsplit=10):
                if re.search(' from ', item, re.I):
                    # 提取SQL段中所有from，join的个数
                    join_list = re.findall(r'(?<!extract) (from)(?!_)', item, re.I)
                    join_list.extend(re.findall(r' join(?=\s|\()', item, re.I))
                    target_list.append(len(join_list))
                else:
                    target_list.append(0)
        target_dict = {}
        for item in target_list:
            if target_dict.get(item, None):
                target_dict[item] += 1
            else:
                target_dict[item] = 1
        return target_dict
        # return dict(Counter(target_list))
        
    def get_group_by_count(self, file_name):
        """
        目前只能处理group by一个字段中有不超过两个括号的情况，如果有多个括号则匹配失败
        功能：从标准化后的py脚本中获取每一段SQL的group by字段个数
        入参：file_name，文件的绝对路径
        返回值：以字典的形式返回每一段中group by字段个数
        """
        content = self.get_re_py_content(file_name)
        # print(content)
        code_segment = re.findall(r'"""(.*?)"""', content)
        target_list = []
        for seg in code_segment:
            # 拆分union
            for item in re.split('union', seg, maxsplit=10):
                if re.search('group by', item, re.I):
                    for entry in re.split('group by', item, re.I)[1:]:
                        cnt_left = 0
                        tt = []
                        for i in entry:
                            if i == ')' and not cnt_left:
                                break
                            elif i == ')':
                                cnt_left -= 1
                            elif i == '(':
                                cnt_left += 1
                            elif not cnt_left:
                                tt.append(i)
                        target_list.append(len(re.sub(r'order by.*', '', "".join(tt)).split(',')))
        target_dict = {}
        for item in target_list:
            if target_dict.get(item, None):
                target_dict[item] += 1
            else:
                target_dict[item] = 1
        return target_dict
        # return dict(Counter(target_list))

    def get_temp_table_count(self, file_name):
        """
        功能：获取标准化后的文件内容中临时表个数,可处理子查询
        入参：file_name，文件的绝对路径
        返回值：临时表个数
        """
        content = self.get_re_py_content(file_name)
        target_list = re.findall(r'(create_temp_table|create[a-zA-Z]*TempView)', content, re.I)
        # 下面是每段SQL中子查询个数的
        code_segment = re.findall(r'"""(.*?)"""', content)
        # target_list = []
        for seg in code_segment:
            # 如果出现括号内有select这个关键词认定这是个临时表
            target_list.extend(re.findall(r'[\(]\s?(select.+?)\s?[\)]', seg , re.DOTALL))
        return len(target_list)

    def get_sql_function_count(self, file_name):
        """
        (count\(|avg\(|max\(|min\(|sum\(|approx_count_distinct\(|collect_list\(|collect_set\(|corr|countDistinct\(|CUME_DIST() OVER\(|covar_pop\(|covar_samp\(|)
        (mean\(|sumDistinct\(|variance\(|var_samp\(|var_pop\(|stddev_pop\(|stddev_samp\(|stddev\(|skewness\(|kurtosis\(|grouping_id\(|grouping\()
        (last\(|first\(|currentRow\(|rank\(|dense_rank\(|row_number\(|percent_rank\(|lag\(|lead\(|ntile\(|unboundedFollowing\()
        功能：获取标准化后的文件内容中每一段SQL用到的函数个数
        通过字母和下划线组合再接左括号以及over关键字的判断得到函数个数
        入参：file_name，文件的绝对路径
        返回值：字典形式，key是函数个数，value是有相同函数个数的SQL段的个数，比如说1:2的意思是，出现一个函数的SQL段有两个
        """
        content = self.get_re_py_content(file_name)
        # print(content)
        code_segment = re.findall(r'"""(.*?)"""', content)
        target_list = []
        # 常见函数
        # pattern = r"""(last\(|first\(|currentRow\(|rank\(|dense_rank\(|row_number\(|percent_rank\(|lag\(|lead\(|ntile\(|unboundedFollowing\(|mean\(|sumDistinct\(|variance\(|var_samp\(|var_pop\(|stddev_pop\(|stddev_samp\(|stddev\(|skewness\(|kurtosis\(|grouping_id\(|grouping\(|count\(|avg\(|max\(|min\(|sum\(|approx_count_distinct\(|collect_list\(|collect_set\(|corr|countDistinct\(|cume_dist\(|covar_pop\(|covar_samp\()"""
        for seg in code_segment:
            # 拆分union
            for item in re.split('union' , seg, maxsplit=100):
                # target_list.append(len(re.findall(pattern, item, re.I)))
                # print( re.sub(r"('.*?')", '', item))
                # print("\n")
                # print(re.findall(r'(\w+\().*?(>=over|)', re.sub(r"('.*?')", '', item), re.I))
                target_list.append(len([item for item in re.findall(r'(\w+\().*?(?=over|)',\
                     re.sub(r"('.*?')", '', item), re.I)\
                     if 'partition' not in item and 'in(' not in item]))
        # print(target_list)
        target_dict = {}
        for item in target_list:
            if target_dict.get(item, None):
                target_dict[item] += 1
            else:
                target_dict[item] = 1
        return target_dict
        # return dict(Counter(target_list))
    
    def get_py_dot_complex(self, file_name, dot_dir):
        """
        版本1 SQL代码段的名称必须不一致
        对文件的要求是每个SQL段的名字应该是不同的，不要都是sql这个名字
        获得py文件中源表和目标表生成的dot文件
        入参:file_name，py文件的绝对路径;dot_dir 要输出的dot文件路径
        返回值：无
        """
        content = self.get_re_py_content(file_name)
        # print(content)
        lines = ["digraph g {", ]
        temp_dict = {}
        entry_dict = {}
        for sql_name, tmp_table_name in re.findall(r'create_temp_table\(\s?(\w+).*?["|\']([a-zA-Z0-9_]+?)["|\']', content, re.I):
            entry_dict[sql_name] = tmp_table_name
        for sql_name, code_segment in re.findall(r'([\w_]+)\s?=\s?"""(.*?)"""', content, re.I): #[1:]:
            sql_name = sql_name.lower()
            source_table = re.findall(r'(?<!extract) from ([{}\w\.]+) (?!import)', code_segment, re.I)
            source_table.extend(re.findall(r'join ([{}\w\.]+)', code_segment, re.I))
            if not entry_dict.get(sql_name, None):
                continue
            target_table = entry_dict[sql_name]
            lines.extend(['"{0}" -> "{1}"'.format(ret, target_table) for ret in source_table \
                 if ret != target_table or (ret == target_table and '.' in ret)])
            # 处理子查询
            for a, b in re.findall(r'([a-zA-Z0-9_]+) as\s?[(]\s?(.*?from.*?)\s?[)]\s?(?=,|s)', code_segment, re.I):
                s = re.findall(r'(?<!extract) from ([{}\w\.]+)\s?(?!import)', b, re.I)
                s.extend(re.findall(r'join ([{}\w\.]+)', b, re.I))
                lines.extend(['"{0}" -> "{1}"'.format(ret, a.lower()) for ret in s])
        
        for code_segment in re.findall(r'"""(\s*insert.*?)"""', content, re.I):#[1:]:
            # print(code_segment)
            target_table = re.findall(r'(?<=into) table ([{}\w]+\.\w+)', code_segment, re.I)
            target_table.extend(re.findall(r'(?<=overwrite) table ([{}\w]+\.\w+)', code_segment, re.I))
            source_table = re.findall(r'(?<!extract) from ([{}\w\.]+) (?!import)', code_segment, re.I)
            source_table.extend(re.findall(r'join ([{}\w\.]+)', code_segment, re.I))
            lines.extend(['"{0}" -> "{1}"'.format(ret, target_table[0].lower()) for ret in source_table])
        lines.append('}')
        dirname, filename = os.path.split(file_name)
        # 这里认定文件名的格式为file_name.filetype，即文件名中出现数字字母和下划线
        file_name = filename.split('.')[0]
        with open(os.path.join(dot_dir,file_name + '.dot'), 'w', encoding='UTF-8') as f:
            f.write("\n".join(lines))
        #生成每个文件对应dag图对应的png
        os.system('dot {0} -T png -o {0}.png'.format(os.path.join(dot_dir,file_name + '.dot')))

    def get_py_dot_complex_test(self, file_name, dot_dir):
        """
        版本2 SQL代码段后直接接注册临时表的代码
        对文件的要求是每个SQL段的后面必须是有create_temp_table这样的字眼
        获得py文件中源表和目标表生成的dot文件
        入参:file_name，py文件的绝对路径;dot_dir 要输出的dot文件路径
        返回值：无
        """
        content = self.get_re_py_content(file_name)
        # print(content)
        lines = ["digraph g {", ]
        # temp_dict = {}
        # entry_dict = {}
        # pprint.pprint(re.findall(r'"""(.*?)""".*?(?=create_temp_table).*?["|\']([a-zA-Z0-9_]+?)["|\']', content, re.I))
        # print("\n")
        # for sql_name, tmp_table_name in re.findall(r'create_temp_table\(\s?(\w+).*?["|\']([a-zA-Z0-9_]+?)["|\']', content, re.I):
        #     entry_dict[sql_name] = tmp_table_name
        pprint.pprint(re.findall(r'"""(.*?)""".*?create([a-z_]*)(?=table|view).*?["|\']([a-zA-Z0-9_]+?)["|\']', content, re.I))#[1:]:)
        for code_segment, xx, table_name in re.findall(r'"""(.*?)""".*?create([a-z_]*)(?=table|view).*?["|\']([a-zA-Z0-9_]+?)["|\']', content, re.I): #[1:]:
            # pprint.pprint(code_segment)
            source_table = re.findall(r'(?<!extract) from ([{}\w\.]+) (?!import)', code_segment, re.I)
            source_table.extend(re.findall(r'join ([{}\w\.]+)', code_segment, re.I))
            # if not entry_dict.get(sql_name, None):
            #     continue
            # target_table = entry_dict[sql_name]
            lines.extend(['"{0}" -> "{1}"'.format(ret, table_name) for ret in source_table])
            # 处理子查询
            for a, b in re.findall(r'([a-zA-Z0-9_]+) as\s?[(]\s?(.*?from.*?)\s?[)]\s?(?=,|s)', code_segment, re.I):
                s = re.findall(r'(?<!extract) from ([{}\w\.]+)\s?(?!import)', b, re.I)
                s.extend(re.findall(r'join ([{}\w\.]+)', b, re.I))
                lines.extend(['"{0}" -> "{1}"'.format(ret, a.lower()) for ret in s])
        
        for code_segment in re.findall(r'"""(\s*insert.*?)"""', content, re.I):#[1:]:
            # print(code_segment)
            target_table = re.findall(r'(?<=into) ([{}\w]+\.\w+)', code_segment, re.I)
            target_table.extend(re.findall(r'(?<=overwrite) table ([{}\w]+\.\w+)', code_segment, re.I))
            source_table = re.findall(r'(?<!extract) from ([{}\w\.]+) (?!import)', code_segment, re.I)
            source_table.extend(re.findall(r'join ([{}\w\.]+)', code_segment, re.I))
            lines.extend(['"{0}" -> "{1}"'.format(ret, target_table[0].lower()) for ret in source_table])
        lines.append('}')
        pprint.pprint(lines)
        dirname, filename = os.path.split(file_name)
        # 这里认定文件名的格式为file_name.filetype，即文件名中出现数字字母和下划线
        file_name = filename.split('.')[0]
        with open(os.path.join(dot_dir,file_name + '.dot'), 'w', encoding='UTF-8') as f:
            f.write("\n".join(lines))
        #生成每个文件对应dag图对应的png
        os.system('dot {0} -T png -o {0}.png'.format(os.path.join(dot_dir,file_name + '.dot')))

    def get_py_dot_complex_test_test(self, file_name, dot_dir):
        """
        版本3 SQL代码段的顺序和对应临时表的顺序必须一致
        对文件的要求是每个SQL段的后面必须是有create_temp_table这样的字眼
        获得py文件中源表和目标表生成的dot文件
        入参:file_name，py文件的绝对路径;dot_dir 要输出的dot文件路径
        返回值：无
        """
        content = self.get_re_py_content(file_name)
        # print(len(re.findall(r'(""".*?""")', content)))
        cc = re.findall(r'(""".*?""")', content)
        # print(len(re.split(r'""".*?"""',content, maxsplit=100)[1:]))
        # pprint.pprint()
        skip_list = []
        for idx,item in enumerate(re.split(r'""".*?"""',content, maxsplit=100)[1:]):
            if 'create' not in item:
                skip_list.append(idx)
        # print(len([item for idx,item in enumerate(cc) if idx not in skip_list]))
        # print(re.findall(r'(""".*?""").*?(?=create)', content))
        # print(content)
        lines = ["digraph g {", ]
        # temp_dict = {}
        # entry_dict = {}
        # pprint.pprint(re.findall(r'"""(.*?)""".*?(?=create_temp_table).*?["|\']([a-zA-Z0-9_]+?)["|\']', content, re.I))
        # print("\n")
        tmp_table_list = re.findall(r'create([a-z_]*)(?=table|view).*?["|\']([a-zA-Z0-9_]+?)["|\']', content, re.I)
        # pprint.pprint(tmp_table_list)
        # for tmp_table_name in re.findall(r'create_temp_table.*?["|\']([a-zA-Z0-9_]+?)["|\']', content, re.I):
        #     entry_dict[sql_name] = tmp_table_name
        # new_content = [item for item in re.findall(r'"""(.*?)"""', content, re.I)\
        #      if 'select' in item and 'insert' not in item]
        new_content = [item for idx,item in enumerate(cc) if idx not in skip_list]
        # pprint.pprint(new_content)
        # for idx, code_segment in enumerate(re.findall(r'"""(.*?)"""', content, re.I)): #[1:]:
        for idx, code_segment in enumerate(new_content):
            # if idx>=len(tmp_table_list):
            #     break
            source_table = re.findall(r'(?<!extract) from ([{}\w\.]+) (?!import)', code_segment, re.I)
            source_table.extend(re.findall(r'join ([{}\w\.]+)', code_segment, re.I))
            lines.extend(['"{0}" -> "{1}"'.format(ret, tmp_table_list[idx][1]) for ret in source_table])
            # 处理子查询
            for a, b in re.findall(r'([a-zA-Z0-9_]+) as\s?[(]\s?(.*?from.*?)\s?[)]\s?(?=,|s)', code_segment, re.I):
                s = re.findall(r'(?<!extract) from ([{}\w\.]+)\s?(?!import)', b, re.I)
                s.extend(re.findall(r'join ([{}\w\.]+)', b, re.I))
                lines.extend(['"{0}" -> "{1}"'.format(ret, a.lower()) for ret in s])
        
        for code_segment in re.findall(r'"""(\s*insert.*?)"""', content, re.I):#[1:]:
            # print(code_segment)
            target_table = re.findall(r'(?<=into) ([{}\w]+\.\w+)', code_segment, re.I)
            target_table.extend(re.findall(r'(?<=overwrite) table ([{}\w]+\.\w+)', code_segment, re.I))
            if not target_table:
                print("目标表为空了，继续")
                print(code_segment)
                continue
            source_table = re.findall(r'(?<!extract) from ([{}\w\.]+) (?!import)', code_segment, re.I)
            source_table.extend(re.findall(r'join ([{}\w\.]+)', code_segment, re.I))
            lines.extend(['"{0}" -> "{1}"'.format(ret, target_table[0].lower()) for ret in source_table])
        lines.append('}')
        # pprint.pprint(lines)
        dirname, filename = os.path.split(file_name)
        # 这里认定文件名的格式为file_name.filetype，即文件名中出现数字字母和下划线
        file_name = filename.split('.')[0]
        dot_file = os.path.join(dot_dir,file_name + '.dot')
        with open(dot_file, 'w', encoding='UTF-8') as f:
            f.write("\n".join(lines))
        #生成每个文件对应dag图对应的png
        os.system('dot {0} -T png -o {0}.png'.format(dot_file))
        # os.startfile(dot_file)
    
        # for item in temp_list:
        #     if 'tmp' in item:
        #         return True
        # return False
    
    def get_py_dot_simple(self, file_name):
        """
        获得py文件中源表和目标表生成的dot文件，通过节点融合的方式来处理一个脚本中插入了多个目标表的情况
        入参:file_name，py文件的绝对路径
        返回值：列表，边的列表
        """
        target_list = []
        tmp_list = []
        with open(file_name, 'r', encoding = 'UTF-8') as f:
            for line in f.readlines()[1:-1]:
                if '->' in line and '.' in line.split('->')[1]:
                    target_list.append(line.strip())
                else:
                    tmp_list.append(line.strip())
        while tmp_list != []:
            add_list = []
            del_list = []
            target_del_list = []
            for line in target_list:
                left, right = [item.strip() for item in line.split('->')]
                for i in tmp_list:
                    tmp_left, tmp_right = [item.strip() for item in i.split('->')]
                    if left == tmp_right:
                        add_list.append('{0} -> {1}'.format(tmp_left, right))
                        del_list.append(i)
                        target_del_list.append(line)
            if not add_list:
                break
            for item in target_del_list:
                if item in target_list:
                    target_list.remove(item)
            target_list.extend(add_list)
            for item in del_list:
                del_list.remove(item)
        # pprint.pprint(target_list)
        return target_list

    def check_cached_table(self, file_name):
        """
        获取使用了两次及以上的表
        输入：file_name, py文件的绝对路径
        返回值：重复的表名组成的集合
        """
        content = self.get_re_py_content(file_name)
        # 排除含有extra，import，Unix等子串的行，获取from后面的原表，删除临时表，source_table这个属于特例，某个文件中用到了这个
        sources_table = re.findall(r"(?<!extract) from ([{}\w\.]+) (?!import)", content, re.I)
        # 获取join后面的原表，删除临时表
        sources_table.extend(re.findall(r' join ([{}\w\.]+)', content, re.I))
        repeat_table = set()
        temp_list = []
        for item in sources_table:
            if item not in temp_list:
                temp_list.append(item)
            else:
                repeat_table.add(item)
        return repeat_table

    def get_cache_table(self, file_name):
        """
        仅支持通过SQL方式缓存和使用.cacheTable这两种方式缓存
        获取py文件中缓存临时表使用情况，考虑到缓存和释放缓存的顺序没有必然关系，所以就不要求缓存和释放的顺序了
        入参: file_name, 返回文件的名称
        返回值： 返回一个诸如  |文件名|没有缓存的临时表|缓存了但是没有释放的|没缓存但是也释放的| 这样的字符串,以换行符结尾
        """
        content = self.get_re_py_content(file_name)
        cache_table = set(re.findall(r'(?<!un)cache\s?(?=lazy|)\s?table ([{}\w\.]+)', content, re.I))
        cache_table.update(set(re.findall(r'\.cachetable\(\s?["|\']([{}\w\.]+)["|\']\s?\)', content, re.I)))
        uncache_table = set(re.findall(r'uncache table ([{}\w\.]+)', content, re.I))
        uncache_table.update(set(re.findall(r'\.uncachetable\(\s?["|\']([{}\w\.]+)["|\']\s?\)', content, re.I)))
        # cache_table = set(r{uncache_table}")
        repeat_table = self.check_cached_table(file_name)
        lines = [os.path.basename(file_name), ]
        if not cache_table - uncache_table and not repeat_table - cache_table:
            lines.extend([" "]*3)
        else:
            if repeat_table - cache_table:
                # print("没有缓存的表为", repeat_table - set(cache_table))
                lines.append(",".join(list(repeat_table - cache_table)))
            else:
                lines.append(' ')
            lines.append(",".join(list(cache_table - uncache_table)))
            lines.append(",".join(list(uncache_table - cache_table)))
            # set_1 = set(cache_table) - set(uncache_table)
            # set_2 = set(uncache_table) - set(cache_table)
        return f"|{'|'.join(lines)}|\n"

    def get_distinct_count(self, file_name):
        """
        获取py文件中每段的SQL的distinct个数
        入参: file_name, py文件的绝对路径
        返回值： distinct次数
        """
        return len(re.findall(r' distinct ', self.get_re_py_content(file_name), re.I))

    def get_hard_code(self, file_name):
        """
        获取py文件中所有的硬编码，ip,层,日期等
        入参: file_name, py文件的绝对路径
        返回值： 出现的硬编码列表
        """
        content = self.get_re_py_content(file_name)
        # ip
        hard_code_list = re.findall(r'(\d{3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', content)
        # 层
        hard_code_list.extend([item[0] for item in re.findall(r'((ods|edw|edw_ai|ods_standard|dm|rst)\.\w+)', content)])
        # hard_code_list.extend([item for item in re.findall(r'(\w+\.\w+)', content) if 'ods.' in item or 'edw.' in item or 'dm.' in item or 'rst.' in item or 'edw_ai.' in item or 'ods_standard.' in item])
        # 日期
        hard_code_list.extend([item for item in re.findall(r'(\d{4}-\d{2}-\d{2})', content) if item != '9999-12-31'])

        return hard_code_list


class statistics_target(content_process):

    def __init__(self,):
        super(statistics_target, self).__init__()
        # super().run(dir_name)
    def get_statistics_join_count(self, dir_name):
        """
        统计一个目录下的所有脚本中每一段SQL，join的个数情况，输出一个markdown文件
        入参：dir_name，py文件夹的绝对路径
        返回值：无
        """
        dir_dict = {}
        for entry in os.scandir(dir_name):
            if entry.name.endswith('.py'):
                dir_dict[entry.name] = super(statistics_target, self).get_join_count(os.path.join(dir_name, entry.name))
        target_file = os.path.join(dir_name, 'statistics_join_count.md')
        with open(target_file, 'w', encoding='UTF-8') as f:
            out_col = set()
            out_col.update([i for item in dir_dict.values() for i in item])
            out_col = list(out_col)
            out_col.sort(reverse = True)
            # print(out_col)
            first_line = "|次数|"+"|".join([str(item) for item in out_col])+"|\n"
            f.write(first_line)
            sep = ['---']*len(out_col)
            f.write("|---|"+"|".join(sep)+"|\n")
            # 以ddl中表名为标准，同步脚本中和Excel中的表名
            for key,value in dir_dict.items():
                # d = dict(value)
                line = [key,]
                for col in out_col:
                    if col in value:
                        line.append(value[col])
                    else:
                        line.append(0)
                f.write("|"+"|".join([str(i) for i in line])+"|\n")
            print("write statistics_join_count.md Done!")
        os.startfile(target_file)

    def get_statistics_temp_table_count(self, dir_name):
        """
        统计文件夹下所有py文件中临时表个数，输出一个markdown文件
        入参:dir_name，py文件夹的绝对路径
        返回值：无
        """
        dir_dict = {}
        for entry in os.scandir(dir_name):
            if entry.name.endswith('.py'):
                # content = content_process().get_re_py_content(os.path.join(dir_name, entry.name))
                dir_dict[entry.name] = super(statistics_target, self).get_temp_table_count(os.path.join(dir_name, entry.name))
        target_file = os.path.join(dir_name, 'statistics_temp_table_count.md')
        with open(target_file, 'w', encoding='UTF-8') as f:
            # out_col = set()
            # out_col.update([i for item in dir_dict.values() for i in item])
            # out_col = list(out_col)
            # out_col.sort(reverse = True)
            # print(out_col)
            first_line = "|表名|临时表个数|\n|---|---|\n"
            f.write(first_line)
            for key,value in dir_dict.items():
                line = [key, str(value)]
                f.write("|"+"|".join(line)+"|\n")
            print("write statistics_temp_table_count.md Done!")
        os.startfile(target_file)

    def get_statistics_group_by_count(self, dir_name):
        """
        统计文件夹下所有py文件中每个SQL段group by字段个数，输出一个markdown文件
        入参:dir_name，py文件夹的绝对路径
        返回值：无
        """
        dir_dict = {}
        for entry in os.scandir(dir_name):
            if entry.name.endswith('.py'):
                # content = content_process().get_re_py_content(os.path.join(dir_name, entry.name))
                dir_dict[entry.name] = super(statistics_target, self).get_group_by_count(os.path.join(dir_name, entry.name))
        target_file = os.path.join(dir_name, 'statistics_group_by_count.md') 
        with open(target_file, 'w', encoding='UTF-8') as f:
            out_col = set()
            out_col.update([i for item in dir_dict.values() for i in item])
            out_col = list(out_col)
            out_col.sort(reverse = True)
            # print(out_col)
            first_line = "|次数|"+"|".join([str(item) for item in out_col])+"|\n"
            f.write(first_line)
            sep = ['---']*len(out_col)
            f.write("|---|"+"|".join(sep)+"|\n")
            # 以ddl中表名为标准，同步脚本中和Excel中的表名
            for key,value in dir_dict.items():
                # d = dict(value)
                line = [key,]
                for col in out_col:
                    if col in value:
                        line.append(value[col])
                    else:
                        line.append(0)
                f.write("|"+"|".join([str(i) for i in line])+"|\n")
            print("write statistics_group_by_count.md Done!")
        os.startfile(target_file)

    def get_statistics_diff_ddl_excel_py(self, ddl_file_name, excel_file_name, py_dir):
        """
        功能：分析ddl，Excel，py脚本三者字段的差异，并将结果写入markdown文档
        传参：建表语句SQL脚本路径，Excel文件路径，py脚本目录
        返回值：空
        """
        # 获取ddl脚本中表和相应的字段
        ddl_dict = super(statistics_target, self).get_ddl_dict(ddl_file_name)
        # 获取Excel数据字典中每个表和字段
        excel_dict = super(statistics_target, self).get_excel_dict(excel_file_name)
        # 获取py脚本中目标表和字段
        py_dir_dict = super(statistics_target, self).get_py_dir_dict(py_dir)
        dir_name, file_name = os.path.split(ddl_file_name)
        target_file = os.path.join(dir_name, 'diff_of_ddl_excel_py.md')
        with open(target_file, 'w', encoding='UTF-8') as f:
            first_line = "|ddl表|Excel中的差异|脚本中的差异|交集|\n|--|--|--|--|\n"
            f.write(first_line)
            # 以ddl中表名为标准，同步脚本中和Excel中的表名
            for key, value in ddl_dict.items():
                table_name = key
                intersection = list(value & excel_dict.get(key, set()) & py_dir_dict.get(key, set()))
                t1 = ['-' + item for item in set(value) - set(excel_dict.get(key, set())) if item] 
                t1.extend(['+' + item for item in set(excel_dict.get(key, set())) - set(value) if item])
                t2 = ['-' + item for item in set(value) - set(py_dir_dict.get(key, set())) if item]
                t2.extend(['+' + item for item in set(py_dir_dict.get(key, set())) - set(value) if item])
                line="|{base}|{t1}|{t2}|{intersection}|".format(base=key,intersection=",".join(intersection),t1=",".join(t1),t2=",".join(t2))
                f.write(line+'\n')
            print("write diff_of_ddl_excel_py.md Done!")
        os.startfile(target_file)

    def get_statistics_origin_target_table(self, dir_name):
        """
        统计文件夹下所有py文件中源表和目标表，输出一个markdown文件
        入参:dir_name，py文件夹的绝对路径
        返回值：无
        """
        target_file = os.path.join(dir_name, 'statistics_origin_target_table.md')
        with open(target_file, 'w', encoding='UTF-8') as f:
            first_line = "|文件名|源表|目标表|\n|---|---|---|\n"
            f.write(first_line)
            for entry in os.scandir(dir_name):
                if entry.name.endswith('.py'):
                    line = [entry.name, ]
                    sources_table = super(statistics_target, self).get_origin_table(os.path.join(dir_name, entry.name))
                    line.append(",".join(sources_table))
                    target_table = super(statistics_target, self).get_target_table(os.path.join(dir_name, entry.name))
                    line.append(",".join(target_table))
                    f.write("|"+"|".join(line)+"|\n")
            print("write statistics_origin_target_table.md Done!")
        os.startfile(target_file)

    def get_py_dir_dot_test(self, dir_name, dot_dir):
        """
        生成文件夹下每个py脚本对应的dot文件(含临时表)，这些dot文件都放在dot_dir下
        入参:dir_name，py文件夹的绝对路径;dot_dir输出的dot文件目录的绝对路径
        返回值：无
        """
        dot_dir = os.path.join(dir_name, 'dot')
        os.makedirs(dot_dir, exist_ok = True)
        for entry in os.scandir(dir_name):
            if entry.is_dir() and entry.name!='dot':
                # 递归子文件夹
                self.get_py_dir_dot(os.path.join(dir_name, entry.name), dot_dir)
            elif entry.is_file() and entry.name.endswith('.py'):
                # 生成每一个文件对应的dot文件
                print(os.path.join(dir_name, entry.name), dot_dir)
                super(statistics_target, self).get_py_dot_complex_test_test(os.path.join(dir_name, entry.name), dot_dir)
        self.get_merge_py_dir_dot(dot_dir)
    
    def get_py_dir_dot(self, dir_name):
        """
        生成文件夹下每个py脚本对应的dot文件(含临时表)，这些dot文件都放在dot_dir下
        入参:dir_name，py文件夹的绝对路径;dot_dir输出的dot文件目录的绝对路径
        返回值：无
        """
        dot_dir = os.path.join(dir_name, 'dot')
        os.makedirs(dot_dir, exist_ok = True)
        for entry in os.scandir(dir_name):
            if entry.is_dir() and entry.name!='dot':
                # 递归子文件夹
                self.get_py_dir_dot(os.path.join(dir_name, entry.name), dot_dir)
            elif entry.is_file() and entry.name.endswith('.py'):
                # 生成每一个文件对应的dot文件
                print(os.path.join(dir_name, entry.name))
                super(statistics_target, self).get_py_dot_complex_test_test(os.path.join(dir_name, entry.name), dot_dir)
        self.get_merge_py_dir_dot(dot_dir)
        
    def get_merge_py_dir_dot(self, dot_dir):
        """
        合并所有文件，生成一个大图
        入参:dot_dir 输出的dot文件目录的绝对路径
        返回值：无
        """
        # dot_dir = os.path.join(dir_name, 'dot')
        # os.makedirs(dot_dir, exist_ok=True)
        target_lines = set()
        for entry in os.scandir(dot_dir):
            if entry.is_file() and entry.name.endswith('.dot') and entry.name != 'res.dot':
                # 递归子文件夹
                print(os.path.join(dot_dir, entry.name))
                target_lines.update(self.get_py_dot_simple(os.path.join(dot_dir, entry.name)))
        print("生成所有的边")
        pprint.pprint(target_lines)
        base_dot = ["digraph g { rankdir=LR;ranksep=10;node [ style = invis ];", ]
        target_dict = {'ods':[], 'edw':[], 'edw_ai':[], 'dm':[], 'rst':[], 'oth':[]}
        try:
            for line in target_lines:
                for item in [item.strip() for item in line.split('->')]:
                    # 给每一个元素分层
                    if 'edw' in item and 'edw_ai' not in item:
                        if item not in target_dict['edw']:
                            target_dict['edw'].append(item)
                    elif 'edw_ai' in item:
                        if item not in target_dict['edw_ai']:
                            target_dict['edw_ai'].append(item)
                    elif 'ods' in item:
                        if item not in target_dict['ods']:
                            target_dict['ods'].append(item)
                    elif 'dm' in item:
                        if item not in target_dict['dm']:
                            target_dict['dm'].append(item)
                    elif 'rst' in item:
                        if item not in target_dict['rst']:
                            target_dict['rst'].append(item)
                    else:
                        if item not in target_dict['oth']:
                            target_dict['oth'].append(item)
        except Exception as e:
            print(f'发生错误了老哥.错误信息如下{e}')
        finally:
            print("\n")
        # 给各层分配各颜色
        color = {'ods':'pink', 'edw':'yellow', 'edw_ai':'green', 'dm':'purple', 'rst':'beige', 'oth':'red'}
        # tt = ['ods':{},'edw':{}, 'edw_ai':{}, 'dm':{},'rst':{}]
        key_ = ['rst', 'dm', 'edw_ai', 'edw', 'ods', 'oth']
        # 按层次插入文件，控制生成的dag图中各层次的布局
        for index, class_value in enumerate(key_):
            # 生成子图,想给每层的元素做个排序，使得在dag中节点有顺序，但是无效
            key, value = class_value,target_dict[class_value]
            # 给每个后面的节点一个颜色
            rank = ['node[shape = record,style=filled,width=5;color={0}]'.format(color[key]),';{rankdir=LR; rank=same;edge[constraint=false];',]
            rank.extend([item + """[group=left][pos="{0},-{1}!"];\n""".format(idx,index) for idx, item in enumerate(value)])
            rank.append('}')
            base_dot.append("\n".join(rank))
        base_dot.extend(list(target_lines))
        base_dot.append("}")
        try:
            with open(os.path.join(dot_dir, 'res.dot'), 'w') as f:
                # 写入最终的dot文件
                f.write("\n".join(base_dot))
            # 生成png图片
            print("开始生成目标png")
            os.system('dot {0} -T png -o {0}.png'.format(os.path.join(dot_dir, 'res.dot')))
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
            os.startfile(os.path.join(dot_dir, 'res.dot')+'.png')
     
    def get_statistics_sql_function_count(self,dir_name):
        """
        统计文件夹下所有py文件中每个SQL段函数个数，输出一个markdown文件
        入参:dir_name，py文件夹的绝对路径
        返回值：无
        """
        dir_dict = {}
        for entry in os.scandir(dir_name):
            if entry.name.endswith('.py'):
                # content = content_process().get_re_py_content(os.path.join(dir_name, entry.name))
                dir_dict[entry.name] = super(statistics_target, self).get_sql_function_count(os.path.join(dir_name, entry.name))
        target_file = os.path.join(dir_name, 'statistics_sql_function_count.md')
        with open(target_file, 'w', encoding='UTF-8') as f:
            out_col = set()
            out_col.update([i for item in dir_dict.values() for i in item])
            out_col = list(out_col)
            out_col.sort(reverse = True)
            # print(out_col)
            first_line = "|次数|"+"|".join([str(item) for item in out_col])+"|\n"
            f.write(first_line)
            sep = ['---']*len(out_col)
            f.write("|---|"+"|".join(sep)+"|\n")
            # 以ddl中表名为标准，同步脚本中和Excel中的表名
            for key, value in dir_dict.items():
                # d = dict(value)
                line = [key, ]
                for col in out_col:
                    if col in value:
                        line.append(value[col])
                    else:
                        line.append(0)
                f.write("|"+"|".join([str(i) for i in line])+"|\n")
        os.startfile(target_file)

    def get_statistics_py_dir_cache_table(self, dir_name):
        """
        得到目录下所有py文件缓存表使用情况,如果想更进一步实现在哪里是最佳的缓存位置和释放位置，感觉很难，
        因为我们可以获取到缓存表但是没有得到相应的位置，也就是说不知道这个表是在哪里缓存的
        入参:dir_name py文件目录的绝对路径
        返回值：无
        """
        target_file = os.path.join(dir_name, 'statistics_py_dir_cache_table.md')
        with open(target_file, 'w', encoding='UTF-8') as f:
            first_line = "|文件名|没有缓存的临时表|缓存了但是没有释放的|没缓存但是也释放的|\n|---|---|---|---|\n"
            f.write(first_line)
            for entry in os.scandir(dir_name):
                if entry.is_file() and entry.name.endswith('.py'):
                    print(os.path.join(dir_name, entry.name))
                    line = super(statistics_target, self).get_cache_table(os.path.join(dir_name, entry.name))
                    f.write(line)
            print("write statistics_py_dir_cache_table.md Done!")
        os.startfile(target_file)

    def get_statistics_distinct_count(self, dir_name):
        """
        得到目录下所有py文件distinct个数
        入参:dir_name py文件目录的绝对路径
        返回值：无
        """
        target_file = os.path.join(dir_name, 'statistics_distinct_count.md')
        with open(target_file, 'w', encoding='UTF-8') as f:
            first_line = "|文件名|distinct个数|\n|---|---|\n"
            f.write(first_line)
            for entry in os.scandir(dir_name):
                if entry.is_file() and entry.name.endswith('.py'):
                    file_name = os.path.join(dir_name, entry.name)
                    print(file_name)
                    cnt = super(statistics_target, self).get_distinct_count(file_name)
                    line = f'|{file_name}|{cnt}|\n'
                    f.write(line)
            print("write statistics_distinct_count.md Done!")
        os.startfile(target_file)

    def get_statistics_hard_code(self, dir_name):
        """
        得到目录下所有py文件硬编码使用情况,主要是ip，层，日期
        入参:dir_name py文件目录的绝对路径
        返回值：无
        """
        target_file = os.path.join(dir_name, 'statistics_hard_code.md')
        with open(target_file, 'w', encoding='UTF-8') as f:
            first_line = "|文件名|硬编码|\n|---|---|\n"
            f.write(first_line)
            for entry in os.scandir(dir_name):
                if entry.is_file() and entry.name.endswith('.py'):
                    file_name = os.path.join(dir_name, entry.name)
                    print(file_name)
                    hard_code = super(statistics_target, self).get_hard_code(file_name)
                    line = f'|{file_name}|{",".join(hard_code)}|\n'
                    f.write(line)
            print("write statistics_hard_code.md Done!")
        os.startfile(target_file)