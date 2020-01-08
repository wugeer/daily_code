import xlrd
import xlwt
import datetime


def read_excel(file_name,
               start_row=0,
               start_column=0,
               time_col=None,
               sheet_name=None,
               skip_sheet=None):
    """
    通过xlrd读取小的Excel(大的Excel可能会内存爆炸)，可指定从那几行那几列开始, 跳过的sheet页, 获取时间列的数据
    :param file_name: excel文件路径
    :param start_row: 从哪一行开始，默认是为0
    :param start_col: 从那一列开始，默认是为0
    :param time_col: 时间列的列名列表，默认为空
    :param skip_sheet: 跳过的sheet页的页名的列表, 默认为空
    :param sheet_name: 指定sheet页的页名, 默认为空
    :return list: 列表，每一行是该文件有效的sheet的每一行，
    """
    # 打开文件得到workbook
    workbook = xlrd.open_workbook(file_name)
    # 获取所有的sheet页名字
    all_worksheets = workbook.sheet_names()
    # 存储除跳过的所有行和列的所有数据
    sheet_rows = []
    if sheet_name is None:
        # 遍历所有sheet页
        for worksheet_name in all_worksheets:
            # 如果sheet页是跳过的sheet页，则继续循环
            if skip_sheet is not None and worksheet_name in skip_sheet:
                continue
            # print(worksheet_name)
            worksheet = workbook.sheet_by_name(worksheet_name)
            # 遍历sheet页中每一个元素，从指定行列开始，并对时间列就行处理
            for row in range(start_row, worksheet.nrows):
                row_container = []
                for col in range(start_column, worksheet.ncols):
                    item = worksheet.cell(row, col).value
                    if row != 0 and col in time_col:
                        # 处理时间类型的列
                        item = datetime.datetime(*xlrd.xldate_as_tuple(item, workbook.datemode))
                    row_container.append(item)
                sheet_rows.append(row_container)
    else:
        # print(worksheet_name)
        worksheet = workbook.sheet_by_name(worksheet_name)
        # 遍历sheet页中每一个元素，从指定行列开始，并对时间列就行处理
        for row in range(start_row, worksheet.nrows):
            row_container = []
            for col in range(start_column, worksheet.ncols):
                item = worksheet.cell(row, col).value
                if row != 0 and col in time_col:
                    # 处理时间类型的列
                    item = datetime.datetime(*xlrd.xldate_as_tuple(item, workbook.datemode))
                row_container.append(item)
            sheet_rows.append(row_container)
    return sheet_rows


def write_excel(content,
                file_name,
                sheet_name='Sheet1',
                mode='w',
                encoding='utf-8'):
    workbook = xlwt.Workbook()  # 新建一个工作簿
    sheet = workbook.add_sheet(sheet_name)  # 在工作簿中新建一个表格
    if mode == 'a': 
        pass
