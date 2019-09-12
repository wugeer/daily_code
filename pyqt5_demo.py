import sys
import os
from PyQt5.QtWidgets import QMainWindow, QAction, QMenu, QApplication, QDialog, QLabel 
from PyQt5.QtWidgets import (QWidget, QLabel, QLineEdit, QPushButton,QFileDialog,QMessageBox,
    QTextEdit, QGridLayout, QApplication,QCheckBox)
from PyQt5 import QtCore, QtGui
from code_review_xhw import content_process, statistics_target
from PyQt5.QtCore import QCoreApplication,Qt


class qt_mainwindow(QMainWindow):

    def __init__(self):
        super().__init__()

        self.initUI()


    def initUI(self):         

        menubar = self.menuBar()

        docMenu = menubar.addMenu('检查文档')
        diffDocAct = QAction('三者对比', self)   
        diffDocAct.triggered.connect(self.diff_excel_ddl_py_dir)
        docMenu.addAction(diffDocAct)

        schemaMenu = menubar.addMenu('层指标')
        schemaTargetAct = QAction('层指标', self) 
        schemaTargetAct.triggered.connect(self.get_schema_target)            
        schemaMenu.addAction(schemaTargetAct)

        dagMenu = menubar.addMenu('dag图')
        schemaAct = QAction('层', self)      
        fileAct = QAction('单文件', self)   
        schemaAct.triggered.connect(self.get_dag_schema)   
        fileAct.triggered.connect(self.get_dag_py)   
        dagMenu.addAction(schemaAct)
        dagMenu.addAction(fileAct)

        helpMenu = menubar.addMenu('帮助')
        versionAct = QAction('版本', self)
        versionAct.triggered.connect(self.get_help)   
        helpMenu.addAction(versionAct)

        self.setGeometry(300, 300, 300, 200)
        self.setWindowTitle('code review小工具')    
        self.show()
    def diff_excel_ddl_py_dir(self):
        # dialog=QDialog()
        obj = diff_excel_ddl_py()
        obj.setupUi()
        # print(1)
    def get_schema_target(self):
        obj = schema_target()
        obj.setupUi()
    def get_dag_schema(self,):
        obj = graph_dialog()
        obj.setup_schema_ui()
    def get_dag_py(self,):
        obj = graph_dialog()
        obj.setup_single_file_Ui()
    def get_help(self,):
        obj = help_dialog()
        obj.setupUi()

class diff_excel_ddl_py(QDialog):
    def setupUi(self):
        # self.Dialog = Dialog
        self.setWindowTitle("检查文档")
        self.setFixedSize(400, 150)
        grid = QGridLayout()
        grid.setSpacing(10)
        
        excel_label = QLabel("Excel文件:")
        ddl_label = QLabel("ddl文件:")
        py_dir_label = QLabel("py文件夹:")

        self.excel_edit = QLineEdit()
        self.ddl_edit = QLineEdit()
        self.py_dir_edit = QLineEdit()

        excel_button = QPushButton("选择文件")
        ddl_button = QPushButton("选择文件")
        py_dir_button = QPushButton("选择文件夹")

        excel_button.clicked.connect(self.get_excel_file)
        ddl_button.clicked.connect(self.get_ddl_file)
        py_dir_button.clicked.connect(self.get_py_dir)

        comfirm_button = QPushButton("运行")
        quit_button = QPushButton("退出")

        comfirm_button.clicked.connect(self.run)
        quit_button.clicked.connect(self.close)

        grid.addWidget(excel_label, 1, 0)
        grid.addWidget(self.excel_edit, 1, 1)
        grid.addWidget(excel_button, 1, 2)

        grid.addWidget(ddl_label, 2, 0)
        grid.addWidget(self.ddl_edit, 2, 1)
        grid.addWidget(ddl_button, 2, 2)

        grid.addWidget(py_dir_label, 3, 0)
        grid.addWidget(self.py_dir_edit,3, 1)
        grid.addWidget(py_dir_button, 3, 2)

        grid.addWidget(comfirm_button, 4, 0)
        grid.addWidget(quit_button, 4, 1)
        self.setLayout(grid) 
        self.setWindowFlags(QtCore.Qt.WindowStaysOnTopHint)#设置窗体总显示在最上面
        # self.retranslateUi(Dialog)
        # Dialog.setWindowModality(Qt.ApplicationModal)
        self.exec_()
        QtCore.QMetaObject.connectSlotsByName(self)
    def get_excel_file(self,):
        file_name = QFileDialog.getOpenFileName(None, 'Open file', r'c:\\')[0]
        self.excel_edit.setText(file_name)
        # print(file_name)
    def get_ddl_file(self,):
        file_name = QFileDialog.getOpenFileName(None, 'Open file', r'c:\\')[0]
        self.ddl_edit.setText(file_name)
    def get_py_dir(self,):
        dir_name = QFileDialog.getExistingDirectory(None, 'Open file', r'c:\\')
        # print(dir_name)
        self.py_dir_edit.setText(dir_name)
    def run(self):
        try:
            obj = statistics_target()
            # print(self.excel_edit.get())
            obj.get_statistics_diff_ddl_excel_py(self.ddl_edit.text(),self.excel_edit.text(),\
                self.py_dir_edit.text())
            QMessageBox.information(self, '提示', '程序执行成功') 
        except Exception as e:
            QMessageBox.critical(self, '错误', f"错误信息为{e}")
    # def retranslateUi(self, Dialog):
    #     _translate = QtCore.QCoreApplication.translate
    #     Dialog.setWindowTitle(_translate("Dialog", "Dialog"))
    #     self.pushButton.setText(_translate("Dialog", "PushButton"))
class schema_target(QDialog):
    def setupUi(self):
        # self.Dialog = Dialog
        self.setWindowTitle("层指标")
        # self.resize(400, 300)
        self.setFixedSize(400, 200)
        grid = QGridLayout()
        grid.setSpacing(10)
        
        py_dir_label = QLabel("py文件夹:")
        self.py_dir_edit = QLineEdit()
        py_dir_button = QPushButton("选择文件")

        self.qc_group_by = QCheckBox("group by 字段")
        self.qc_temp_table = QCheckBox("临时表个数")
        self.qc_join = QCheckBox("join个数")
        self.qc_origin_target_table = QCheckBox("源表和目标表")
        self.qc_sql_func = QCheckBox("sql函数个数")
        self.qc_all = QCheckBox("反选")
        py_dir_button.clicked.connect(self.get_py_dir)
        self.qc_all.clicked.connect(self.chang_status)

        comfirm_button = QPushButton("运行")
        quit_button = QPushButton("退出")

        comfirm_button.clicked.connect(self.run)
        quit_button.clicked.connect(self.close)

        grid.addWidget(py_dir_label, 1, 0)
        grid.addWidget(self.py_dir_edit, 1, 1)
        grid.addWidget(py_dir_button, 1, 2)

        grid.addWidget(self.qc_group_by, 2, 0)
        grid.addWidget(self.qc_temp_table, 2, 2)
        # grid.addWidget(ddl_button, 2, 2)

        grid.addWidget(self.qc_join, 3, 0)
        grid.addWidget(self.qc_origin_target_table,3, 2)
        # grid.addWidget(py_dir_button, 3, 2)
        grid.addWidget(self.qc_sql_func, 4, 0)
        grid.addWidget(self.qc_all,4, 2)

        grid.addWidget(comfirm_button, 5, 0)
        grid.addWidget(quit_button, 5, 1)
        self.setLayout(grid) 
        self.setWindowFlags(QtCore.Qt.WindowStaysOnTopHint)#设置窗体总显示在最上面
        # self.retranslateUi(Dialog)
        # Dialog.setWindowModality(Qt.ApplicationModal)
        self.exec_()
        QtCore.QMetaObject.connectSlotsByName(self)
    def chang_status(self,):
        self.qc_group_by.setChecked(True if self.qc_group_by.checkState() == Qt.Unchecked else False) # if self.qc_group_by.checkState() == Qt.Unchecked else False
        self.qc_temp_table.setChecked(True if self.qc_temp_table.checkState() == Qt.Unchecked else False)
        self.qc_join.setChecked(True if self.qc_join.checkState() == Qt.Unchecked else False)
        self.qc_origin_target_table.setChecked(True if self.qc_origin_target_table.checkState() == Qt.Unchecked else False)
        self.qc_sql_func.setChecked(True if self.qc_sql_func.checkState() == Qt.Unchecked else False)
        # print(self.qc_all.get())
    def get_py_dir(self,):
        dir_name = QFileDialog.getExistingDirectory(None, 'Open file', r'c:\\')
        # print(dir_name)
        self.py_dir_edit.setText(dir_name)
    def run(self):
        try:
            obj = statistics_target()
            dir_name = self.py_dir_edit.text()
            if self.qc_group_by.checkState() == Qt.Checked:
            	obj.get_statistics_group_by_count(dir_name)
            if self.qc_join.checkState() == Qt.Checked:
            	obj.get_statistics_join_count(dir_name)
            if self.qc_temp_table.checkState() == Qt.Checked:
            	obj.get_statistics_temp_table_count(dir_name)
            if self.qc_sql_func.checkState() == Qt.Checked:
            	obj.get_statistics_sql_function_count(dir_name)
            if self.qc_origin_target_table.checkState() == Qt.Checked:
            	obj.get_statistics_origin_target_table(dir_name)
            QMessageBox.information(self, '提示', '程序执行成功') 
        except Exception as e:
            QMessageBox.critical(self, '错误', f"错误信息为{e}")


class graph_dialog(QDialog):
    def setup_schema_ui(self):
        # self.Dialog = Dialog
        self.setWindowTitle("层的dag图")
        # self.resize(400, 300)
        self.setFixedSize(400, 80)
        grid = QGridLayout()
        grid.setSpacing(10)
        
        py_dir_label = QLabel("py文件夹:")
        self.py_dir_edit = QLineEdit()
        py_dir_button = QPushButton("选择文件夹")

        comfirm_button = QPushButton("运行")
        quit_button = QPushButton("退出")

        py_dir_button.clicked.connect(self.get_py_dir)
        comfirm_button.clicked.connect(self.run_schema)
        quit_button.clicked.connect(self.close)

        grid.addWidget(py_dir_label, 1, 0)
        grid.addWidget(self.py_dir_edit, 1, 1)
        grid.addWidget(py_dir_button, 1, 2)

        grid.addWidget(comfirm_button, 2, 0)
        grid.addWidget(quit_button, 2, 1)
        self.setLayout(grid) 
        #设置窗体总显示在最上面
        self.setWindowFlags(QtCore.Qt.WindowStaysOnTopHint)
        self.exec_()
        QtCore.QMetaObject.connectSlotsByName(self)
    def get_py_dir(self,):
        dir_name = QFileDialog.getExistingDirectory(None, 'Open file', r'c:\\')
        # print(dir_name)
        self.py_dir_edit.setText(dir_name)
    def run_schema(self):
        try:
            obj = statistics_target()
            obj.get_py_dir_dot(self.py_dir_edit.text())
            QMessageBox.information(self, '提示', '程序执行成功') 
        except Exception as e:
            QMessageBox.critical(self, '错误', f"错误信息为{e}")

    def setup_single_file_Ui(self):
        # self.Dialog = Dialog
        self.setWindowTitle("文件的dag图")
        # self.resize(400, 300)
        self.setFixedSize(400, 80)
        grid = QGridLayout()
        grid.setSpacing(10)
        
        py_file_label = QLabel("py文件:")
        self.py_file_edit = QLineEdit()
        py_file_button = QPushButton("选择文件")

        comfirm_button = QPushButton("运行")
        quit_button = QPushButton("退出")
        py_file_button.clicked.connect(self.get_py_file)
        comfirm_button.clicked.connect(self.run_single_file)
        quit_button.clicked.connect(self.close)

        grid.addWidget(py_file_label, 1, 0)
        grid.addWidget(self.py_file_edit, 1, 1)
        grid.addWidget(py_file_button, 1, 2)

        grid.addWidget(comfirm_button, 2, 0)
        grid.addWidget(quit_button, 2, 1)
        self.setLayout(grid) 
        #设置窗体总显示在最上面
        self.setWindowFlags(QtCore.Qt.WindowStaysOnTopHint)
        self.exec_()
        QtCore.QMetaObject.connectSlotsByName(self)
    def get_py_file(self,):
        file_name = QFileDialog.getOpenFileName(None, 'Open file', r'c:\\')[0]
        # print(dir_name)
        self.py_file_edit.setText(file_name)
    def run_single_file(self):
        try:
            obj = content_process()
            file_name = self.py_file_edit.text()
            # dot_dir = os.path.abspath(os.path.dirname(file_name))
            dot_dir = os.path.join(os.path.split(file_name)[0], 'dot')
            os.makedirs(dot_dir,exist_ok=True)
            obj.get_py_dot_complex_test_test(file_name, dot_dir)
            dirname, filename = os.path.split(file_name)
            file_ = filename.split('.')[0]
            dot_file = os.path.join(dot_dir,file_ + '.dot')
            os.startfile(dot_file+'.png')
            QMessageBox.information(self, '提示', '程序执行成功') 
        except Exception as e:
            QMessageBox.critical(self, '错误', f"错误信息为{e}")


class help_dialog(QDialog):
    def setupUi(self):
        QMessageBox.information(self, '帮助', '当前版本为0.1') 
      
if __name__ == '__main__':

    app = QApplication(sys.argv)
    ex = qt_mainwindow()
    sys.exit(app.exec_())