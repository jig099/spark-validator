import ast
from ast import NodeTransformer, fix_missing_locations
import astunparse
'''
import inspect
import test_program1
source = inspect.getsource(test_program1)
'''


class Trans1(NodeTransformer):
    def is_read(self, node):
        if isinstance(node, ast.Assign) and isinstance(node.targets[0].ctx, ast.Store):
            if isinstance(node.value, ast.Call):
                func = node.value.func
                while isinstance(func, ast.Attribute):
                    if func.attr == 'read':
                        return True
                    func = func.value
        return False
        '''Assign(targets=[Name(id='df', ctx=Store())], 
        value=Call(func=Attribute(value=Attribute(value=Name(id='spark', ctx=Load()), attr='read', ctx=Load()), 
        attr='csv', ctx=Load()), 
        args=[Str(s='output.txt')], keywords=[])) '''          
    
    def visit_FunctionDef(self, node):
        assign_list = []
        for i, n in enumerate(node.body):
            if self.is_read(n):
                var_name = n.targets[0].id
                new_assign = ast.Assign(targets=[ast.Name(id=var_name, ctx=ast.Store)], \
                                        value=ast.Call(func=ast.Attribute(value=ast.Name(id=var_name, ctx=ast.Load), \
                                                       attr='sample', ctx=ast.Load), args=[], keywords=[]))
                assign_list.append((i+1, new_assign)) 
        for stmt in assign_list[::-1]:
            node.body.insert(stmt[0], stmt[1])
        return node


tree = ast.parse(open('tests/user_program/test_program1.py').read())
'''
print(tree.body)
print(tree.body[1].body[1].targets[0].id)
print(type(tree.body[1].body[1].targets[0].ctx))
assert(isinstance(tree.body[1].body[1].targets[0].ctx, ast.Store))
#print(ast.dump(tree))
assert(isinstance(tree.body[1], ast.FunctionDef))

print(ast.dump(tree))
'''
t1 = Trans1()
new_tree = fix_missing_locations(t1.visit(tree))
print(ast.dump(new_tree))

print(astunparse.unparse(new_tree))

