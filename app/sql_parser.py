keywords = ["select","from","create","table","index"]

class KeywordUsedAsIdentifierNameError(Exception):
    def __init__(self,msg="A keyword cannot be used as a name for columns, tables or indexes"):
        self.message = msg
        super().__init__(self.message)
        
class NoTokenFoundError(Exception):
    def __init__(self,msg="No token found"):
        self.message = msg
        super().__init__(self.message)
        
class QueryActionAlreadySetError(Exception):
    def __init__(self,msg="This token already has an action set"):
        self.message = msg
        super().__init__(self.message)
        
class InvalidQuerySyntaxError(Exception):
    def __init__(self,msg="There is invalid syntax in the query"):
        self.message = msg
        super().__init__(self.message)
        
class SQLAction:
    NONE         = 0
    SELECT       = 1
    CREATE_TABLE = 2
    CREATE_INDEX = 3
    
class TokenStream:
    def __init__(self,tokens):
        self.idx = -1
        self.stream = tokens
        
    def get_next(self):
        self.idx += 1
        if self.idx >= len(self.stream):
            raise NoTokenFoundError
        return self.stream[self.idx]
    
    def has_next(self):
        if self.idx+1 < len(self.stream):
            return True
        return False
    
    def peek_next(self):
        if self.idx+1 >= len(self.stream):
            raise NoTokenFoundError
        return self.stream[self.idx+1]
    
    def skip_unneeded_tokens(self):
        if not self.has_next():
            raise NoTokenFoundError
        while self.stream[self.idx+1] in ["primary","key","autoincrement","not","null",","]:
            self.idx += 1
            
class WhereCmp:
    EQ = 0
    NE = 1
    LT = 2
    GT = 3
    LE = 4
    GE = 5
    
class QueryCond:
    def __init__(self,col,op,val):
        self.col = col
        self.op = self._cmp_op(op)
        self.value = val
    
    def _cmp_op(self,op):
        if op == "==" or op == "=":
            return WhereCmp.EQ
        if op == "!=":
            return WhereCmp.NE
        if op == "<":
            return WhereCmp.LT
        if op == ">":
            return WhereCmp.GT
        if op == "<=":
            return WhereCmp.LE
        if op == ">=":
            return WhereCmp.GE
        
    def __str__(self):
        return self.col + " " + str(self.op) + " " + self.value
        
    def comp(self,val):
        if WhereCmp.EQ:
            return self.value == val
        if WhereCmp.NE:
            return self.value != val
        if WhereCmp.LT:
            return self.value < val
        if WhereCmp.GT:
            return self.value > val
        if WhereCmp.LE:
            return self.value <= val
        if WhereCmp.GE:
            return self.value >= val

class ParsedQuery:
    def __init__(self):
        self.action = SQLAction.NONE
        self.all_cols = False
        self.count_cols = False
        self.col_names = []
        self.col_dtypes = []
        self.table = None
        self.cond = None
        self.index = None
    
    def has_action(self):
        return self.action != SQLAction.NONE

def parse(sql_str):
    token_stream = TokenStream(sql_str.lower().replace("(","( ").replace(")"," )").replace(","," , ").split())
    p_query = ParsedQuery()
    while token_stream.has_next():
        token = token_stream.get_next()
        if "select" == token:
            if p_query.has_action():
                raise QueryActionAlreadySetError
            p_query.action = SQLAction.SELECT
            col_name = token_stream.get_next()
            if col_name == "*":
                p_query.all_cols = True
            elif col_name == "count(*)":
                p_query.count_cols = True
            else:
                col_names = []
                while True:
                    if col_name in keywords:
                        raise KeywordUsedAsIdentifierNameError
                    col_names.append(col_name)
                    if token_stream.peek_next() == ",":
                        token_stream.get_next()
                        col_name = token_stream.get_next()
                    else:
                        break
                p_query.col_names = col_names
        elif "from" == token:
            tbl_name = token_stream.get_next()
            if tbl_name in keywords:
                raise KeywordUsedAsIdentifierNameError
            p_query.table = tbl_name
        elif "create" == token:
            if p_query.has_action():
                raise QueryActionAlreadySetError
            p_query.action = SQLAction.CREATE_TABLE
            action = token_stream.get_next()
            if action == "table":
                tbl_name = token_stream.get_next()
                if tbl_name in keywords:
                    raise KeywordUsedAsIdentifierNameError
                p_query.table = tbl_name
                if token_stream.get_next() != "(":
                    raise InvalidQuerySyntaxError("Expected a '(' after the table name")
                while token_stream.peek_next() != ")":
                    col_name = token_stream.get_next()
                    data_type = token_stream.get_next()
                    if token_stream.peek_next() != ")":
                        token_stream.skip_unneeded_tokens()
                    p_query.col_names.append(col_name)
                    p_query.col_dtypes.append(data_type)
            elif action == "index":
                if p_query.col_names:
                    print("HAS COLUMNS:",p_query.col_names)
                idx_name = token_stream.get_next()
                if idx_name in keywords:
                    raise KeywordUsedAsIdentifierNameError
                p_query.index = idx_name
                if token_stream.get_next() != "on":
                    raise InvalidQuerySyntaxError("Expected 'on' after the index name")
                tbl_name = token_stream.get_next()
                if tbl_name in keywords:
                    raise KeywordUsedAsIdentifierNameError
                p_query.table = tbl_name
                if token_stream.get_next() != "(":
                    raise InvalidQuerySyntaxError("Expected a '(' after the table name")
                while token_stream.peek_next() != ")":
                    col_name = token_stream.get_next()
                    p_query.col_names.append(col_name)
            else:
                raise InvalidQuerySyntaxError("Create keyword must be followed by either table or index") 
        elif "where" == token:
            col_name = token_stream.get_next()
            cmp_op = token_stream.get_next()
            value = token_stream.get_next()
            if value.startswith("'"):
                if value.endswith("'"):
                    value = value[1:-1] #.title()
                else:
                    while not value.endswith("'"):
                        value += " " + token_stream.get_next()
                    #value = value[1:-1].title()
            p_query.cond = QueryCond(col_name,cmp_op,value)
    return p_query
            