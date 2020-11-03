import sys
from PyQt5.QtWidgets import *
from PyQt5.QtCore import pyqtSignal

class CheckBoxGroup(QFrame):

    sigCbkStateChanged = pyqtSignal(str, int)

    def __init__(self, parent=None, dictCbkPara={}, dictLayoutPara={}):
        # 父类构造
        super(CheckBoxGroup,self).__init__(parent)
        self._gridLayout = QGridLayout()
        self.setLayout(self._gridLayout)
        self._nLayoutCol = 5
        self.updateUI(dictCbkPara, dictLayoutPara)

    def updateUI(self, dictCbkPara={}, dictLayoutPara={}):
        # 初始化勾选框
        self._dictCbkPara = dictCbkPara
        # 初始化布局参数
        try:
            self._nLayoutCol = dictLayoutPara["col"]
        except:
            pass

        # 填加勾选框
        i = 0
        j = 0
        for cbkKey in self._dictCbkPara.keys():
            if j >= self._nLayoutCol:
                i += 1
                j = 0
            cbk = QCheckBox(self._dictCbkPara[cbkKey])
            cbk.stateChanged.connect(self.stateChangeSlot)
            self._gridLayout.addWidget(cbk, i, j)
            cbk.setObjectName(cbkKey)
            j += 1
        self.update()

    def stateChangeSlot(self, nState):
        print(self.sender().objectName(), nState)
        self.sigCbkStateChanged.emit(self.sender().objectName(), nState)
        pass





