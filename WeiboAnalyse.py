#encoding:utf-8
import MySQLdb
import pickle
DEBUG = True
def debug(*argv):
    if not DEBUG:return
    print argv
class node:
    def __init__(self,id):
        self.id = id
        self.children = []
    def addChild(self,child):
        self.children.append(child)
    def getChildren(self):
        return self.children
    def setChildren(self,children):
        self.children = children
        
class Tree:
    def __init__(self):
        self.conn=MySQLdb.connect(host="127.0.0.1",user="root",
                          passwd="g",db="nWeibo",charset='utf8')
    
        self.cursor = self.conn.cursor()
        self.record = open('analysis.txt','w')
        pass
    def createTree(self):
        self.cursor.execute("select rid from weibo where rootId = 0")
        rootIds = self.cursor.fetchall()
        for rootId in rootIds:
            root = node(rootId[0])
            tempfile = open(str(root.id)+'.dat','w')
            self.createChildTree(root)
            pickle.dump(root,tempfile)
    def createChildTree(self,currentNode):
        print currentNode.id
        self.cursor.execute("select rid,parentId from weibo where parentId = %s",currentNode.id)
        items = self.cursor.fetchall()
        for item in items:
            child = node(item[0])
            print '#',child.id
            currentNode.addChild(child)
            self.createChildTree(child)
    def output(self,index,num):
        self.record.write("{0} ---> {1}\n".format(index,num))
    def analyseTree(self,root):
        index = 0
        currentNodes = [root]
        while True:
            self.output(index,len(currentNodes))
            nextNodes = []
            for currentNode in currentNodes:
                nextNodes.extend(currentNode.getChildren())
            if not nextNodes:break
            currentNodes = nextNodes
            index += 1
    def test(self):
        a = node(1)
        for i in range(10):
            b = node(i)
            a.addChild(b)
            for j in range(i):
                c = node(i)
                b.addChild(c)
        import pickle
        pickle.dump(a,open('tes.dat','w'))
        l = pickle.Unpickler(open('tes.dat'))
        c = l.load()
        print c.id
                

if __name__ == "__main__":
    tree = Tree()
    #tree.createTree()
    root = pickle.load(open('1.dat'))
    tree.analyseTree(root)
    #tree.test()
        
            
            
        
