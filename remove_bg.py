from removebg import RemoveBg
import tkinter as tk  # 使用Tkinter前需要先导入
import tkinter.messagebox
from tkinter import filedialog 
import os
import sys


class tkiner_demo(object):
    def __init__(self):
        self.window = tk.Tk()
        # 第2步，给窗口的可视化起名字
        self.window.title('去除图片背景的小工具')
        self.window.resizable(False, False)
        # 第3步，设定窗口的大小(长 * 宽)
        self.window.geometry('400x100')  # 这里的乘是小x


    def paint(self):
        # label设置
        tk.Label(self.window, text='文件:', font=('宋体 常规', 14, "bold"), borderwidth=2).grid(row=0, sticky=tk.E)#.place(x=60, y=30)
        tk.Label(self.window, text='文件夹:', font=('宋体 常规', 14, "bold")).grid(row=1, sticky=tk.E)#.place(x=60, y=70)
        #entry设置
        self.file = tk.StringVar()  # 将输入的注册名赋值给变量
        # self.file.set()  # 将最初显示定为'example@python.com'
        entry_file = tk.Entry(self.window, textvariable=self.file, width=30) 
        entry_file.grid(row=0, column=1)#..place(x=180, y=30)
        tk.Button(self.window, text='选择文件', font=('宋体 常规', 14, "bold"), width=10, height=1, command=self.get_file).grid(row=0, column=2)#.place(x=350, y=310)


        self.dirname = tk.StringVar()  # 将输入的注册名赋值给变量
        # self.dirname.set()  # 将最初显示定为'example@python.com'
        entry_dirname = tk.Entry(self.window, textvariable=self.dirname, width=30) 
        entry_dirname.grid(row=1, column=1)#.place(x=180, y=70)
        tk.Button(self.window, text='选择文件夹', font=('宋体 常规', 14, "bold"), width=10, height=1, command=self.get_dir).grid(row=1, column=2)#.place(x=350, y=310)

        tk.Button(self.window, text='开始去除背景', font=('宋体 常规', 14, "bold"), width=10, height=1, command=self.runninng).place(x=120, y=70)


    def get_file(self):
        local_file = filedialog.askopenfilename()
        # print(local_file)
        # self.file=local_file
        self.file.set(local_file)
        # self.file.set(local_file)
    def get_dir(self):
        local_dir = filedialog.askdirectory()
        self.dirname.set(local_dir)
    def is_pic(self, pic_path):
        for i in ['jpg', 'png']:
            if pic_path.endswith(i):
                return True
        return False
    def runninng(self):
        # pass
        try:
            rmbg = RemoveBg("KT6twuYZcsqhd7JetDZfC46z", "error.log") # 引号内是你获取的API
            if self.file.get():
                print(self.file.get())
                rmbg.remove_background_from_img_file(self.file.get())
                tkinter.messagebox.showinfo('提示','图片成功已去掉背景')
            elif self.dirname.get():
                for pic in os.listdir(self.dirname.get()):
                    if self.is_pic(pic):
                        rmbg.remove_background_from_img_file(os.path.join(self.dirname.get(), pic))
                tkinter.messagebox.showinfo('提示','文件夹下所有图片成功已去掉背景')
            else:
                tkinter.messagebox.showerror('错误', "老哥，你的输入有问题，请检查")
        except Exception as e:
            tkinter.messagebox.showerror('错误', e)
            sys.exit(1)
        finally:
            # tkinter.messagebox.showinfo('程序退出')
            sys.exit(1)
        

if __name__ == "__main__":
    # obj = tkiner_demo()
    # obj.paint()
    tk_demo = tkiner_demo()
    tk_demo.paint()
    # 第6步，主窗口循环显示
    tk_demo.window.mainloop()
    # rmbg = RemoveBg("KT6twuYZcsqhd7JetDZfC46z", "error.log") # 引号内是你获取的API
    # rmbg.remove_background_from_img_file("C:/Users/xhw/Desktop/微信图片_20190724155835.jpg") #图片地址
# print("ok")
    # s=''
    # if s:
    #     print(1)
    # else:
    #     print(0)