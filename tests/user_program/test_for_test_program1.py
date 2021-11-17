import ast
'''
import inspect
import test_program1
source = inspect.getsource(test_program1)
'''
tree = ast.parse(open('tests/user_program/test_program1.py').read())
print(tree)
print(tree.body)
#print(ast.dump(tree))

