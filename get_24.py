"""
author:xiehongwang
desription:给个四个1到10的可重复整数，输出这四个数的四则运算（有括号）结果为24的结果
last_modified:2019-08-15
remark: 缺点：里面写死了，如果考虑括号的只能做四个整数
"""
from itertools import permutations
import pprint
from itertools import chain
import random

def my_all_str(num_list, char_list):
    for item in char_list:
        resa = []
        resa.append("""({0}{4}{1}){5}{2}{6}{3}""".format(*num_list, *item))
        resa.append("""{0}{4}{1}{5}({2}{6}{3})""".format(*num_list, *item))
        resa.append("""{0}{4}({1}{5}{2}){6}{3}""".format(*num_list, *item))
        resa.append("""({0}{4}{1}){5}({2}{6}{3})""".format(*num_list, *item))
        resa.append("""({0}{4}{1}{5}{2}){6}{3}""".format(*num_list, *item))
        resa.append("""{0}{4}({1}{5}{2}{6}{3})""".format(*num_list, *item))
        resa.append("""{0}{4}{1}{5}{2}{6}{3}""".format(*num_list, *item))
        for res in resa: 
            yield res
            
        

def get_all_possible(num_list):
    char_str = ['+', '-', '*', '/']*3
    target_list=[list(item) for item in permutations(num_list)]
    char_list = [list(item) for item in permutations(char_str,3)]
    res = set()
    for num_list in target_list:
        for item in my_all_str(num_list, char_list):
            try:
                if abs(24-eval(item))<=0.001:
                    res.add(item)
            except ZeroDivisionError as e:
                continue
    print(res)
if __name__ =="__main__":
    num_list = [random.randint(1,10) for i in range(4)]
    # num_list = [3,8,3,8]
    print(num_list)
    # 原生方法3.46s
    get_all_possible(num_list)