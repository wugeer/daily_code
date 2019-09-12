import os
from tkinter import *
from tkinter.messagebox import *
from tkinter import filedialog 
from code_review_xhw import content_process, statistics_target
 
class check_document_frame(Frame): # 继承Frame类
	def __init__(self, master=None):
		Frame.__init__(self, master)
		self.root = master #定义内部变量root
		self.grid()
		self.excel_file = StringVar()
		self.ddl_file = StringVar()
		self.py_dir = StringVar()
		# self.createDiffExcelDdlPyPage()
		# self.deductPrice = StringVar()
		# self.createPage()
	def createDiffExcelDdlPyPage(self, ):
		Label(self,text='excel文件:',font=('宋体 常规',14,"bold")).grid(row=1,sticky=W,pady=10,padx=20)
		Entry(self,textvariable=self.excel_file, width=30).grid(row=1, column=1, stick=E)
		Button(self,text='选择文件', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.get_excel_file).grid(row=1, column=2)

		Label(self, text='ddl文件:', font=('宋体 常规', 14, "bold")).grid(row=2, sticky=W,pady=10,padx=20)
		Entry(self, textvariable=self.ddl_file, width=30).grid(row=2, column=1, stick=E)
		Button(self, text='选择文件', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.get_ddl_file).grid(row=2, column=2)

		Label(self, text='py文件夹:', font=('宋体 常规', 14, "bold")).grid(row=3, sticky=W,pady=10,padx=20)
		Entry(self, textvariable=self.py_dir, width=30).grid(row=3, column=1, stick=E)
		Button(self, text='选择文件', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.get_py_dir).grid(row=3, column=2)#.place(x=350, y=310)

		Button(self, text='运行', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.run).grid(row=4, sticky=W, pady=10, padx=20)#.place(x=350, y=310)
		Button(self, text='退出', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.root.destroy).grid(row=4, column=1, stick=E)#.place(x=350, y=310)
		# Label(self, text='输出的指标的对比文件在ddl文件所在的目录', font=('宋体 常规', 14, "bold")).\
		# 	grid(row=5, sticky=W, pady=10)
	def get_excel_file(self,):
		local_file = filedialog.askopenfilename()
		self.excel_file.set(local_file)
	def get_ddl_file(self,):
		local_file = filedialog.askopenfilename()
		self.ddl_file.set(local_file)
	def get_py_dir(self,):
		local_file = filedialog.askdirectory()
		self.py_dir.set(local_file)
	def frame_forget(self,):
		self.grid_forget()
		print("forget")
	def run(self,):
		try:
			obj = statistics_target()
			obj.get_statistics_diff_ddl_excel_py(self.ddl_file.get(),self.excel_file.get(),self.py_dir.get())
			showinfo(title='提示', message='程序执行成功') 
		except Exception as e:
			showerror(title='错误', message=f"错误信息为{e}")
		# finally:
			# self.frame_forget()
 

class schema_target_frame(Frame): # 继承Frame类
	def __init__(self, master=None):
		Frame.__init__(self, master)
		self.root = master #定义内部变量root
		self.grid()
		self.py_dir = StringVar()
		self.group_by_var = IntVar()
		self.temp_table_var = IntVar()
		self.join_var = IntVar()
		self.sql_function_var = IntVar()
		self.origin_target_table_var = IntVar()
		self.cache_table_var = IntVar()
		self.distinct_var = IntVar()
		self.hard_code_var = IntVar()
		self.all_var = IntVar()
		# self.createPage()

	def createSchemaPage(self,):
		Label(self, text='py文件夹:', font=('宋体 常规', 14, "bold")).grid(row=1, sticky=W, pady=10,padx=20)
		Entry(self, textvariable=self.py_dir, width=30).grid(row=1, column=1, stick=E)
		Button(self, text='选择文件', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.get_py_dir).grid(row=1, column=2)#.place(x=350, y=310)

		Checkbutton(self, text='group by 字段',variable=self.group_by_var, onvalue=1, offvalue=0).\
            grid(row=2, sticky=W, pady=10,padx=20)
		Checkbutton(self, text='临时表个数',variable=self.temp_table_var, onvalue=1, offvalue=0).\
            grid(row=2, column=1, stick=W,padx=20)
		Checkbutton(self, text='join 个数',variable=self.join_var, onvalue=1, offvalue=0).\
            grid(row=2, column=2, stick=W,padx=20)
		
		Checkbutton(self, text='SQL函数个数',variable=self.sql_function_var, onvalue=1, offvalue=0).\
            grid(row=3,sticky=W, pady=10,padx=20)
		Checkbutton(self, text='源表和目标表',variable=self.origin_target_table_var, onvalue=1, offvalue=0).\
            grid(row=3, column=1, stick=W,padx=20)
		Checkbutton(self, text='缓存临时表',variable=self.cache_table_var, onvalue=1, offvalue=0).\
            grid(row=3, column=2, stick=W,padx=20)

		Checkbutton(self, text='distinct个数',variable=self.distinct_var, onvalue=1, offvalue=0).\
            grid(row=4,sticky=W, pady=10, padx=20)
		Checkbutton(self, text='硬编码',variable=self.hard_code_var, onvalue=1, offvalue=0).\
            grid(row=4,column=1, stick=W,padx=20)
		Checkbutton(self, text='反选',variable=self.all_var, onvalue=1, offvalue=0, command=self.check_turn).\
            grid(row=4,column=2, stick=W,padx=20)
		
		Button(self, text='运行', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.run).grid(row=5, sticky=W, pady=10,padx=20)#.place(x=350, y=310)
		Button(self, text='退出', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.root.quit).grid(row=5, column=1, stick=E)#.place(x=350, y=310)
	def get_py_dir(self,):
		local_file = filedialog.askdirectory()
		self.py_dir.set(local_file)
	def check_turn(self, ):
		self.group_by_var.set(1 if not self.group_by_var.get() else 0)
		self.join_var.set(1 if not self.join_var.get() else 0)
		self.temp_table_var.set(1 if not self.temp_table_var.get() else 0)
		self.sql_function_var.set(1 if not self.sql_function_var.get() else 0)
		self.origin_target_table_var.set(1 if not self.origin_target_table_var.get() else 0)
		self.cache_table_var.set(1 if not self.cache_table_var.get() else 0)
		self.distinct_var.set(1 if not self.distinct_var.get() else 0)
		self.hard_code_var.set(1 if not self.hard_code_var.get() else 0)
	def run(self,):
		try:
			obj = statistics_target()
			dir_name = self.py_dir.get()
			# obj.get_statistics_join_count(self.ddl_file.get(),self.excel_file.get(),self.py_dir.get())
			if self.group_by_var.get():
				obj.get_statistics_group_by_count(dir_name)
			if self.join_var.get():
				obj.get_statistics_join_count(dir_name)
			if self.temp_table_var.get():
				obj.get_statistics_temp_table_count(dir_name)
			if self.sql_function_var.get():
				obj.get_statistics_sql_function_count(dir_name)
			if self.origin_target_table_var.get():
				obj.get_statistics_origin_target_table(dir_name)
			if self.cache_table_var.get():
				obj.get_statistics_py_dir_cache_table(dir_name)
			if self.distinct_var.get():
				obj.get_statistics_distinct_count(dir_name)
			if self.hard_code_var.get():
				obj.get_statistics_hard_code(dir_name)
			showinfo(title='提示', message='程序执行成功') 
		except Exception as e:
			showerror(title='错误', message=f"错误信息为{e}")
	def frame_forget(self,):
		self.grid_forget()
		print("forget")

class graph_frame(Frame): # 继承Frame类
	def __init__(self, master=None):
		Frame.__init__(self, master)
		self.root = master #定义内部变量root
		self.grid()
		self.py_dir = StringVar()
		self.py_file = StringVar()
		# self.createPage()
 
	def create_schema_page(self):
		Label(self, text='py文件夹:', font=('宋体 常规', 14, "bold")).grid(row=1, sticky=W, pady=10,padx=20)
		Entry(self, textvariable=self.py_dir, width=30).grid(row=1, column=1, stick=E)
		Button(self, text='选择文件', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.get_py_dir).grid(row=1, column=2)#.place(x=350, y=310)
		Button(self, text='运行', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.run_schema).grid(row=2, sticky=W, pady=10,padx=20)#.place(x=350, y=310)
		Button(self, text='退出', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.root.quit).grid(row=2, column=1, stick=E)#.place(x=350, y=310)
	def create_py_page(self):
		Label(self, text='py文件  :', font=('宋体 常规', 14, "bold")).grid(row=1, sticky=W, pady=10,padx=20)
		Entry(self, textvariable=self.py_file, width=30).grid(row=1, column=1, stick=E)
		Button(self, text='选择文件', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.get_py_file).grid(row=1, column=2)#.place(x=350, y=310)
		Button(self, text='运行', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.run_single_py).grid(row=2, sticky=W, pady=10,padx=20)#.place(x=350, y=310)
		Button(self, text='退出', font=('宋体 常规', 14, "bold"), width=10, height=1,\
             command=self.root.quit).grid(row=2, column=1, stick=E)#.place(x=350, y=310)
	def get_py_dir(self,):
		local_file = filedialog.askdirectory()
		self.py_dir.set(local_file)
	def get_py_file(self,):
		local_file = filedialog.askopenfilename()
		self.py_file.set(local_file)
	def frame_forget(self,):
		self.grid_forget()
		# print("forget")
	def run_schema(self,):
		try:
			obj = statistics_target()
			obj.get_py_dir_dot(self.py_dir.get())
			showinfo(title='提示', message='程序执行成功') 
		except Exception as e:
			showerror(title='错误', message=f"错误信息为{e}")
	def run_single_py(self,):
		try:
			obj = content_process()
			file_name = self.py_file.get()
			# dot_dir = os.path.abspath(os.path.dirname(file_name))
			dot_dir = os.path.join(os.path.split(file_name)[0], 'dot')
			os.makedirs(dot_dir,exist_ok=True)
			obj.get_py_dot_complex_test_test(file_name, dot_dir)
			dirname, filename = os.path.split(file_name)
			file_ = filename.split('.')[0]
			dot_file = os.path.join(dot_dir,file_ + '.dot')
			os.startfile(dot_file+'.png')
			showinfo(title='提示', message='程序执行成功') 
		except Exception as e:
			showerror(title='错误', message=f"错误信息为{e}")


class help_frame(Frame): # 继承Frame类
	def __init__(self, master=None):
		Frame.__init__(self, master)
		self.root = master #定义内部变量root
		self.grid()
		# self.createPage()
	def frame_forget(self,):
		self.grid_forget()
	def createPage(self):
		Label(self, text='版本0.1').grid(row=1, sticky=W, pady=10)
# if __name__ == '__main__':
# 	root = Tk()
# 	root.title('小程序')
# 	obj = check_document_frame(root)
# 	obj.createDiffExcelDdlPyPage()
# 	# time.sleep(4)
# 	# obj.frame_forget()
# 	# print(1)
# 	root.mainloop()
