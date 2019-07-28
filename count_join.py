import re 
import pprint

def get_join_number(target_file):
    with open(target_file, 'r', encoding='UTF-8') as f:
        content = " ".join([item.rstrip() for item in f.readlines() if item.strip() and 'drop' not in item and 'delete' not in item])
    # 获取所有的SQL段
    temp = [i for item in re.findall(r'(create|insert)?([^;]*);', content, re.M|re.DOTALL) for i in item if len(i)>6]
    # print(temp)
    d = {}
    for s in temp:
        # 每个SQL段的表名
        key = [i for item in re.findall(r'(table|into)\s*([a-zA-Z._0-9]*)', s)for i in item if i not in ('table', 'into')][0]
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
        
    pprint.pprint(d)

if __name__ == "__main__":
    target_file = r"""D:\code\test\daily_code\ceshi.py"""
    get_join_number(target_file)