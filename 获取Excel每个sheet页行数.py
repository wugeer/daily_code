import xlrd
import os
# file_name = """D:\gongsi_project\新百伦\客户数据\Daily&Week Sales\月周和日期对应表.xlsx"""
dir_name = """D:\gongsi_project\新百伦\客户数据"""
for entry in os.scandir(dir_name):
    if entry.is_dir():
        continue
    file_name = os.path.join(dir_name, entry.name)
    wb = xlrd.open_workbook(file_name)
    for sheet_name in wb.sheet_names():
        sheet = wb.sheet_by_name(sheet_name)
        print('excel{0} sheet{1} 行数为{2}'.format(entry.name, sheet_name, sheet.nrows-1))

