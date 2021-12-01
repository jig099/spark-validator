import ast
from ast import NodeTransformer, fix_missing_locations
import astunparse
'''
import inspect
import test_program1
source = inspect.getsource(test_program1)
'''


class Transform_Read(NodeTransformer):
    def __init__(self):
        super().__init__()
        self.dataframes_ = []

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
                self.dataframes_.append(var_name)
        for stmt in assign_list[::-1]:
            node.body.insert(stmt[0], stmt[1])
        return node
    
    def visit_Module(self, node):
        assign_list = []
        for i, n in enumerate(node.body):
            if isinstance(n, ast.FunctionDef):
                self.visit_FunctionDef(n)
            if self.is_read(n):
                var_name = n.targets[0].id
                new_assign = ast.Assign(targets=[ast.Name(id=var_name, ctx=ast.Store)], \
                                        value=ast.Call(func=ast.Attribute(value=ast.Name(id=var_name, ctx=ast.Load), \
                                                       attr='sample', ctx=ast.Load), args=[], keywords=[]))
                assign_list.append((i+1, new_assign)) 
                self.dataframes_.append(var_name)
        for stmt in assign_list[::-1]:
            node.body.insert(stmt[0], stmt[1])
        return node

class Transform_Operation(NodeTransformer):
    def __init__(self, dataframes) -> None:
        super().__init__()
        self.dataframes_ = dataframes
        
          
    def visit_Assign(self, node: ast.Assign):
        '''
        Assume each spark function call is assigned to some variable
        Assume only single identifier on the left hand side of '='
        Assume function in form of df = df1.func(args)
        does not handle chained calls
        Modify a call if df1 in a spark dataframe except when func is sample
        '''
        if isinstance(node.value, ast.Call) and \
            isinstance(node.value.func, ast.Attribute) and \
            isinstance(node.value.func.value, ast.Name) and \
            node.value.func.value.id in self.dataframes_ and \
            node.value.func.attr != 'sample':
            
            print('Here')
            var_name = node.targets[0].id
            func_name = node.value.func.attr
            calling_df = node.value.func.value.id
            args = node.value.args
            args.insert(0, ast.Name(id=calling_df, ctx=ast.Load()))
           
            new_assign = ast.Assign(targets=[ast.Name(id=var_name, ctx=ast.Store)], \
                                    value=ast.Call(func=ast.Name(id=func_name, ctx=ast.Load()), args=args, keywords=[]))
            print(astunparse.unparse(new_assign))
            '''args=[Name(id='t1', ctx=Load()), Name(id='t2', ctx=Load())]
            Assign(targets=[Name(id='a', ctx=Store())], 
                value=Call(func=Attribute(value=Name(id='a', ctx=Load()), attr='foo', ctx=Load()), args=[], keywords=[]))
            value=Call(func=Name(id='foo', ctx=Load()), args=[], keywords=[])'''
            return new_assign
        else:
            return node

def translate_spark_program(program):
    '''
    input: program can be either a 
    '''
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
    t1 = Transform_Read()
    new_tree = fix_missing_locations(t1.visit(tree))
    print(astunparse.unparse(new_tree))
    print(t1.dataframes_)
    t2 = Transform_Operation(t1.dataframes_)
    new_tree = fix_missing_locations(t2.visit(new_tree))
    print(astunparse.unparse(new_tree))

