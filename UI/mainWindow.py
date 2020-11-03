import sys
from PyQt5.QtWidgets import QApplication,QMainWindow,QFrame
from PyQt5.QtGui import QIcon
from UI.mainWindow_ui import Ui_MainWindow
from UI.checkBoxGroup import CheckBoxGroup

class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self,parent=None):
        #初始化继承的父类（Qmainwindow）
        super(MainWindow, self).__init__(parent)
        self.setupUi(self)
        # #设置窗口的大小
        # self.resize(400,200)
        # #实例化创建状态栏
        # self.status=self.statusBar()
        self.cbkGroup.updateUI({'a':'aaa','b':'bbb'})

        # #将提示信息显示在状态栏中showMessage（‘提示信息’，显示时间（单位毫秒））
        # self.status.showMessage('这是状态栏提示',4000)
        # #创建窗口标题
        # self.setWindowTitle('PyQt MainWindow例子')

if __name__ == '__main__':
    app = QApplication(sys.argv)
    form = MainWindow()
    form.show()
    sys.exit(app.exec_())