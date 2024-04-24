keywords = ["select","from","create","table"]

class KeywordUsedAsColumnNameError(Exception):
    def __init__(self,msg="A keyword cannot be used as a column name"):
        self.message = msg
        super().__init__(self.message)
        
class KeywordUsedAsTableNameError(Exception):
    def __init__(self,msg="A keyword cannot be used as a table name"):
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
    NONE   = 0
    SELECT = 1
    CREATE = 2
    
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
        return self.stream[self.idx+1]
    
    def skip_unneeded_tokens(self):
        if not self.has_next():
            raise NoTokenFoundError
        while self.stream[self.idx+1] in ["primary","key","key,","autoincrement","autoincrement,","not","null","null,"]:
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
    action = SQLAction.NONE
    all_cols = False
    count_cols = False
    col_names = []
    col_dtypes = []
    table = None
    cond = None
    
    def has_action(self):
        return self.action != SQLAction.NONE

def parse(sql_str):
    token_stream = TokenStream(sql_str.split())
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
                    if col_name.endswith(","):
                        col_names.append(col_name[:-1])
                        if col_names[-1] in keywords:
                            raise KeywordUsedAsColumnNameError
                        col_name = token_stream.get_next()
                    else:
                        if col_name in keywords:
                            raise KeywordUsedAsColumnNameError
                        col_names.append(col_name)
                        break
                p_query.col_names = col_names
        elif "from" == token:
            tbl_name = token_stream.get_next()
            if tbl_name in keywords:
                raise KeywordUsedAsTableNameError
            p_query.table = tbl_name
        elif "create" == token:
            if p_query.has_action():
                raise QueryActionAlreadySetError
            p_query.action = SQLAction.CREATE
            if token_stream.get_next() != "table":
                raise InvalidQuerySyntaxError("Create keyword must be followed by this keyword: table")
            tbl_name = token_stream.get_next()
            if tbl_name in keywords:
                raise KeywordUsedAsTableNameError
            if token_stream.get_next() != "(":
                print("ERROR:",token)
                raise InvalidQuerySyntaxError("Expected a '(' after the table name")
            while token_stream.peek_next() != ")":
                col_name = token_stream.get_next()
                data_type = token_stream.get_next()
                if token_stream.peek_next() != ")":
                    token_stream.skip_unneeded_tokens()
                if data_type.endswith(","):
                    data_type = data_type[:-1]
                p_query.col_names.append(col_name)
                p_query.col_dtypes.append(data_type)
        elif "where" == token:
            col_name = token_stream.get_next()
            cmp_op = token_stream.get_next()
            value = token_stream.get_next()
            if value.startswith("'"):
                if value.endswith("'"):
                    value = value[1:-1].title()
                else:
                    while not value.endswith("'"):
                        value += " " + token_stream.get_next()
                    value = value[1:-1].title()
            p_query.cond = QueryCond(col_name,cmp_op,value)
            #print("CONDITION:",p_query.cond)
    return p_query
            