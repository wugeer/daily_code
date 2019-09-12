import tkinter as tk  # 使用Tkinter前需要先导入
import time
import tkinter.messagebox
from tkinter import filedialog 
from view import *

class tk_ui(object):
	def __init__(self,):
		self.window = tk.Tk()
		self.window.title("code review小工具")
		self.window.geometry("510x230")
		self.menubar = tk.Menu(self.window)
		self.window.resizable(False, False)

		self.check_document_frame = check_document_frame(self.window) # 创建不同Frame
		self.schema_target_frame = schema_target_frame(self.window)
		self.graph_frame = graph_frame(self.window)
		self.help_frame = help_frame(self.window)
		# 检查文档的菜单
		doc_menu = tk.Menu(self.menubar, tearoff=0)  # tearoff意为下拉
		#　将上面定义的空菜单命名为`File`，放在菜单栏中，就是装入那个容器中
		self.menubar.add_cascade(label='检查文档', menu=doc_menu)
		#　在`文件`中加入`新建`的小菜单，即我们平时看到的下拉菜单，每一个小菜单对应命令操作。
		#　如果点击这些单元, 就会触发`self.window.quit`的功能
		doc_menu.add_command(label='三者对比', command=self.diff_ddl_excel_py)
		
		# 层指标菜单
		schema_menu = tk.Menu(self.menubar, tearoff=0)
		self.menubar.add_cascade(label='层指标', menu=schema_menu)
		schema_menu.add_command(label='层指标', command=self.get_schema_target)
		# schema_menu.add_command(label='统计group by', command=self.window.quit)
		# schema_menu.add_command(label='统计join', command=self.window.quit)
		# schema_menu.add_command(label='统计临时表', command=self.window.quit)
		# schema_menu.add_command(label='统计SQL函数', command=self.window.quit)
		# schema_menu.add_command(label='统计源表和目标表', command=self.window.quit)

		# dag图菜单
		graph_menu = tk.Menu(self.menubar, tearoff=0)
		self.menubar.add_cascade(label='dag图', menu=graph_menu)
		graph_menu.add_command(label='层', command=self.get_dag_scheme)
		graph_menu.add_command(label='单个脚本', command=self.get_dag_py)

		# 单表菜单
		# graph_menu = tk.Menu(self.menubar, tearoff=0)
		# self.menubar.add_cascade(label='单表', menu=graph_menu)
		# graph_menu.add_command(label='层', command=self.window.quit)
		# graph_menu.add_command(label='单个脚本', command=self.window.quit)

		# 帮助菜单
		help_menu = tk.Menu(self.menubar, tearoff=0)
		self.menubar.add_cascade(label='帮助', menu=help_menu)
		# help_menu.add_command(label='帮助', command=self.window.quit)
		help_menu.add_command(label='关于', command=self.get_help)

		self.window.config(menu=self.menubar)  # 加上这代码，才能将菜单栏显示
		self.window.mainloop()
	def diff_ddl_excel_py(self,):
		self.check_document_frame.grid_forget()
		self.graph_frame.grid_forget()
		self.schema_target_frame.grid_forget()
		self.help_frame.grid_forget()
		# self.check_document_frame.frame_forget()
		self.check_document_frame.grid()
		self.check_document_frame.createDiffExcelDdlPyPage()
		# print(1)
		# self.check_document_frame.frame_forget()
	def get_schema_target(self,):
		self.graph_frame.grid_forget()
		self.check_document_frame.grid_forget()
		# self.window.update()
		# print(1)
		self.schema_target_frame.grid_forget()
		self.help_frame.grid_forget()
		self.schema_target_frame.grid()
		# for widget in self.check_document_frame.winfo_children():
		# 	widget.destroy()
		self.schema_target_frame.createSchemaPage()
	def get_dag_scheme(self,):
		self.schema_target_frame.grid_forget()
		self.check_document_frame.grid_forget()
		self.help_frame.grid_forget()
		self.graph_frame.grid_forget()
		self.graph_frame.grid()
		self.graph_frame.create_schema_page()
	def get_dag_py(self,):
		self.schema_target_frame.grid_forget()
		self.check_document_frame.grid_forget()
		self.help_frame.grid_forget()
		# self.graph_frame.grid_forget()
		self.graph_frame.grid()
		self.graph_frame.create_py_page()
	def get_help(self,):
		self.schema_target_frame.grid_forget()
		self.check_document_frame.grid_forget()
		self.graph_frame.grid_forget()
		self.help_frame.grid_forget()
		self.help_frame.grid()
		self.help_frame.createPage()

if __name__ == '__main__':
	obj = tk_ui() 

# window = tk.Tk()
# window.title('菜单栏')
# window.geometry('300x200')
# menubar = tk.Menu(window)


# l = tk.Label(window, bg='green', width=25, height=2, text='empty')
# l.pack()
 
# # 下方command参数的self.window.quit函数
# counter = 0
# def self.window.quit():
#     global counter
#     l.config(text='do'+str(counter))
# #　定义一个空的菜单单元
# filemenu = tk.Menu(menubar, tearoff=0)  # tearoff意为下拉
# #　将上面定义的空菜单命名为`File`，放在菜单栏中，就是装入那个容器中
# menubar.add_cascade(label='文件', menu=filemenu)
# #　在`文件`中加入`新建`的小菜单，即我们平时看到的下拉菜单，每一个小菜单对应命令操作。
# #　如果点击这些单元, 就会触发`self.window.quit`的功能
# filemenu.add_command(label='新建', command=self.window.quit)
# filemenu.add_command(label='打开', command=self.window.quit)
# filemenu.add_command(label='保存', command=self.window.quit)
# # 分隔线
# filemenu.add_separator()
# filemenu.add_command(label='退出', command=window.quit)
 
# # 创建编辑菜单
# editmenu = tk.Menu(menubar, tearoff=0)
# menubar.add_cascade(label='编辑', menu=editmenu)
# editmenu.add_command(label='剪切', command=self.window.quit)
# editmenu.add_command(label='复制', command=self.window.quit)
# editmenu.add_command(label='粘贴', command=self.window.quit)
# window.config(menu=menubar)  # 加上这代码，才能将菜单栏显示
# window.mainloop()