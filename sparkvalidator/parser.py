import ast
from ast import NodeTransformer, fix_missing_locations
import astunparse
from os.path import exists
import re
import pdb

implemented_functions = {
                            'join',
                            'reshape',
                            'min',
                            'max',
                            'grouping',
                            'aggregation',
                            'map',
                            'reduce',
                            'count',
                            'sum',
                            'average',
                            'rename',
                            'replaceNull',
                            'sort',
                            'filter'
                        }

class Transform_Read(NodeTransformer):
    def __init__(self, sample_type, args=None):
        super().__init__()
        
        if sample_type == 'stratified' and (not args or 'fractions' not in args or 'column' not in args):
            exit(1)

        self.dataframes_ = []
        self.sample_type_ = sample_type
        self.args = args
        
        self.fraction = 0.1
        self.with_replacement = False
        self.seed = 0

        if args:
            if 'fraction' in args:
                self.fraction = args['fraction']
            if 'seed' in args:
                self.seed = args['seed']
            if 'with_replacement' in args:
                self.with_replacement = args['with_replacement']
    
    def call_sampler(self, df):
        if self.sample_type_ == 'random' : 
            call = ast.Call(func=ast.Name(id='random_sampling', ctx=ast.Load()), \
                            args=[ast.Name(id=df, ctx=ast.Load()), ast.Name(id=self.seed, ctx=ast.Load()), \
                            ast.Name(id=self.with_replacement, ctx=ast.Load()), \
                            ast.Name(id=self.fraction, ctx=ast.Load())], \
                            keywords=[])
        else:
            call = ast.Call(func=ast.Name(id='stratified_sampling', ctx=ast.Load()), \
                            args=[ast.Name(id=df, ctx=ast.Load()), ast.Name(id=self.seed, ctx=ast.Load()), \
                            ast.Name(id=self.args['column'], ctx=ast.Load()), \
                            ast.Name(id=self.args['fractions'], ctx=ast.Load())], \
                            keywords=[])
        return call

    def is_read(self, node):
        if isinstance(node, ast.Assign) and \
          isinstance(node.targets[0].ctx, ast.Store) and \
          isinstance(node.value, ast.Call) and \
          re.match(r'.*spark\.read.*', astunparse.unparse(node.value)) is not None:
            return True
            '''pdb.set_trace()
            func = node.value.func
            while isinstance(func, ast.Attribute):
                if func.attr == 'read':
                    return True
                func = func.value'''
        return False 
    
    def visit_FunctionDef(self, node):
        assign_list = []
        for i, n in enumerate(node.body):
            if self.is_read(n):
                var_name = n.targets[0].id
                new_assign = ast.Assign(targets=[ast.Name(id=var_name, ctx=ast.Store)], \
                                        value=self.call_sampler(var_name))
                '''new_assign = ast.Assign(targets=[ast.Name(id=var_name, ctx=ast.Store)], \
                                        value=ast.Call(func=ast.Attribute(value=ast.Name(id=var_name, ctx=ast.Load), \
                                                       attr='sample', ctx=ast.Load), args=[], keywords=[]))'''
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
            #pdb.set_trace()
            if self.is_read(n):
                var_name = n.targets[0].id
                new_assign = ast.Assign(targets=[ast.Name(id=var_name, ctx=ast.Store)], \
                                        value=self.call_sampler(var_name))
                assign_list.append((i+1, new_assign)) 
                self.dataframes_.append(var_name)
       
        for stmt in assign_list[::-1]:
            node.body.insert(stmt[0], stmt[1])
        
        import_sampler = ast.ImportFrom(module='sparkvalidator.sampler', names=[ast.alias(name='*', asname=None)], level=0)
        import_functions = ast.ImportFrom(module='sparkvalidator.functions', names=[ast.alias(name='*', asname=None)], level=0)
        node.body.insert(0, import_sampler)
        node.body.insert(0, import_functions)
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
        Assume all for function calls s.t. func is in implemented_functions, df is a spark dataframe
        '''
        if isinstance(node.value, ast.Call) and \
            isinstance(node.value.func, ast.Attribute) and \
            isinstance(node.value.func.value, ast.Name) and \
            node.value.func.attr in implemented_functions:
            #node.value.func.value.id in self.dataframes_ and 
            
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
        # elif node.value.func.value.func.value.attr == 'rdd' and \
        #     node.value.func.value.func.attr in implemented_functions:

        else:
            return node

def translate_spark_program(program : str, target_path=None, get_tree=False, sample_type='random', sample_args=None):
    '''
    input: program can be either a file handle or a str type program
    output: the translated program
    '''
    file_exists = exists(program)
    if file_exists:
        tree = ast.parse(open(program).read())
    else:
        tree = ast.parse(program)

    t1 = Transform_Read(sample_type, sample_args)
    new_tree = fix_missing_locations(t1.visit(tree))
    '''print(astunparse.unparse(new_tree))
    print(t1.dataframes_)'''
    t2 = Transform_Operation(t1.dataframes_)
    
    new_tree = fix_missing_locations(t2.visit(new_tree))
    new_program = astunparse.unparse(new_tree)
    
    
    if target_path:
        f = open("target_path", "w")
        f.write(new_program)
        f.close()
    
    if not get_tree:
        return new_program
    else:
        return new_program, new_tree

program = '../tests/user_program/test2.py'
new_program = translate_spark_program(program, target_path='../tests/user_program/test2_parsed.py')
print(new_program)