import sys, struct
import app.sql_parser as sp

from dataclasses import dataclass

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

class PageType:
    InteriorIndex = 0x02
    InteriorTable = 0x05
    LeafPageBitmask = 0x08
    LeafIndex = 0x0a
    LeafTable = 0x0d
    
class UnknownSerialTypeError(Exception):
    def __init__(self,srl_type,msg="Invalid serial type found:"):
        self.message = msg
        self.serial = srl_type
        super().__init__(self.message+" "+str(serial))
    
def read_page(db_file,pg_num,pgsz):
    db_file.seek((pg_num-1)*pgsz)
    return db_file.read(pgsz)

def read_int(page,start,blen):
    return int.from_bytes(page[start:start+blen])

SRL_TYPE_INT_LENS = (1,2,3,4,6,8)

def read_varint(buffer,offset):
    val = 0
    bidx = 0
    USE_NEXT_BYTE = 0x80
    BITS_TO_USE = 0x7F
    byte9used = False
    for bc in range(8):
        byte = buffer[offset+bidx]
        val = (val << 7) | (byte & BITS_TO_USE)
        byte9used = bool(byte & USE_NEXT_BYTE)
        if not byte9used:
            break
        else:
            bidx += 1
    if byte9used:
        byte = buffer[offset+bidx]
        val = (val << 8) | byte
        bidx += 1
    return val, bidx

def parse_record_body(srl_type,page,offset):
    if not srl_type:
        return None, 0
    elif srl_type > 0 and srl_type < 7:
        srl_len = SRL_TYPE_INT_LENS[srl_type-1]
        return read_int(page,offset,srl_len), srl_Len
    elif srl_type == 7:
        return struct.unpack(">f",page[offset:offset+8]), 8
    elif srl_type == 8 or srl_type == 9:
        return srl_type&1, 0
    elif srl_type >= 12 and srl_type&1==0:
        datalen = (srl_type-12)>>1
        return page[offset:offset+datalen], datalen
    elif srl_type >= 13 and srl_type&1==1:
        datalen = (srl_type-13)>>1
        return page[offset:offset+datalen].decode(), datalen
    else:
        raise UnknownSerialTypeError(srl_type)
    
def parse_cellLT(offset,page):
    payload_size, bytes_read = read_varint(page,offset)
    offset += bytes_read
    row_id, bytes_read = read_varint(page,offset)
    offset += bytes_read
    record_hdr_sz, bytes_read = read_varint(page,offset)
    record_body_start = offset+record_hdr_sz
    offset += bytes_read
    serial_types = []
    while offset < record_body_start:
        srl, bytes_read = read_varint(page,offset)
        serial_types.append(srl)
        offset += bytes_read
    record = []
    for srl_type in serial_types:
        value, val_len = parse_record_body(srl_type,page,offset)
        record.append(value)
        offset += val_len
    return record, row_id

def parse_ICell(offset,page):
    payload_size, bytes_read = read_varint(page,offset)
    offset += bytes_read
    record_hdr_sz, bytes_read = read_varint(file)
    record_body_start = offset+record_hdr_sz
    offset += bytes_read
    serial_types = []
    while offset < record_body_start:
        srl, bytes_read = read_varint(file)
        serial_types.append(srl)
        offset += bytes_read
    record = []
    for srl_type in serial_types:
        value, val_len = parse_record_body(srl_type,page,offset)
        record.append(value)
        offset += val_len
    return record

def get_table_info(cell_ptrs,dbfile,tbl_name):
    for cell_ptr in cell_ptrs:
        record, row_id = parse_cell(cell_ptr,dbfile)
        if record[1] == tbl_name:
            return {"rootpage":record[3],"desc":sp.parse(record[4].lower().replace("(","( ").replace(")"," )").replace(",",", "))}
        
def get_records(start_offset,cells,db_file,tdesc,query_ref):
    records = []
    for c_ptr in cells:
        cell, row_id = parse_cell(start_offset+c_ptr,db_file)
        record = {}
        for col_name, col_value in zip(tdesc.col_names,cell):
            if col_name == "id":
                record[col_name] = col_value or row_id
            else:
                record[col_name] = col_value
        if query_ref.cond and query_ref.cond.col in record.keys():
            if query_ref.cond.comp(record[query_ref.cond.col]):
                continue
        records.append(list(record.values()))
    return records

def parse_interior_header(page):
    cell_amt = read_int(page,3,2)
    last_pg_num = read_int(page,8,4)
    cell_ptrs = [read_int(page,i,2) for i in range(12,12+(cell_amt<<1),2)]
    return cell_ptrs, last_pg_num

def parse_TKCell(offset,page):
    rowid, bytes_read = read_varint(page,offset)
    return rowid

def parse_ITCells(page,cell_ptrs):
    pages = []
    keys = []
    for c_ptr in cell_ptrs:
        pg_num = read_int(page,c_ptr,4)
        cell = parse_TKCell(c_ptr+4,page)
        pages.append(pg_num)
        keys.append(cell)
    return pages, keys

def travel_tables(pg_num,db_file,pg_sz,tdesc,query_ref):
    page = read_page(db_file,pg_num,pg_sz)
    if page[0] == PageType.InteriorTable:
        cell_ptrs, last_pg_num = parse_interior_header(page)
        pages, keys = parse_ITCells(page,cell_ptrs)
        records = []
        for pg in pages:
            records.extend(travel_pages(pg,db_file,pg_sz,tdesc,query_ref))
    elif page_type == PageType.LeafTable:
        return get_records(pg_num,cell_ptrs,db_file,tdesc,query_ref)
    
def get_db_schema(page,cell_ptrs):
    db_objs = {"tables":{},"indexes":{}}
    for cptr in cell_ptrs:
        cell = parse_cellLT(cptr,page)
        if cell[1].startswith("sqlite_"):
            continue
        obj = {cell[1]:{"pg_num":cell[3],"query":sp.parse(cell[4])}}
        if cell[0] == "table":
            db_objs["tables"].append(obj)
        elif cell[0] == "index":
            obj["table"] = cell[2]
            db_objs["indexes"].append(obj)
        else:
            print("Invalid database object",cell[0])
    return db_objs

def get_valid_index(indexes,table,col):
    for index in indexes:
        if index.table != table:
            continue
        if len(index.cols) != 1:
            continue
        if index.cols[0] != col:
            continue
        if col in table.cols:
            return index
    return None

def parse_LIHeader(page):
    cell_amt = read_int(page,3,2)
    return [read_int(page,i,2) for i in range(8,8+(cell_amt<<1),2)]

def parse_IICells(page,cell_ptrs):
    pages = []
    keys = []
    for c_ptr in cell_ptrs:
        pg_num = read_int(page,c_ptr,4)
        cell = parse_ICell(c_ptr+4,page)
        pages.append(pg_num)
        keys.append(cell)
    return pages, keys

def binary_search_first(cell_ptrs,start,end,page,val):
    mid_cell = (start+end)>>2
    cell = parse_ICell(cell_ptrs[mid_cell],page)
    if start == end:
        if cell[0] == val:
            return mid_cell, cell
        else:
            return None, None
    if val <= cell:
        return binary_search_first(cell_ptrs,start,mid_cell,page,val)
    else:
        return binary_search_first(cell_ptrs,mid_cell+1,end,page,val)

def travel_idxs(qry_cond,pg_num,db_file,pg_sz):
    rowids = []
    page = read_page(db_file,pg_num,pg_sz)
    col_val = qry_cond.value
    searching = True
    search_started = False
    if page[0] == PageType.InteriorIndex:
        cell_ptrs, last_pg_num = parse_interior_header(page)
        pages, keys = parse_IICells(page,cell_ptrs)
        del page
        idx = 0
        key_amt = len(keys)
        while idx < key_amt and col_val > keys[idx][0]:
            idx += 1
        while searching and idx < key_amt and col_val <= keys[idx][0]:
            more_rowids, searching, search_started = travel_idxs(qry_cond,pages[idx],db_file,pg_sz)
            idx += 1
            rowids.extend(more_rowids)
            if searching:
                if col_val == keys[idx][0]:
                    rowids.append(keys[idx][1])
                else:
                    searching = False
        if searching:
            travel_idxs(qry_cond,last_pg_num,db_file,pg_sz)
        return rowids, searching, search_started
    elif page[0] == PageType.LeafIndex:
        cell_ptrs = parse_LIHeader(page)
        if search_started:
            cptr = cell_ptrs[0]
            first_idx = 0
            cell = parse_ICell(cptr,page)
        else:
            first_idx, cell = binary_search_first(cell_ptrs,0,len(cell_ptrs)-1,page,col_val)
        if not first_idx:
            print("Error find value")
            return []
        else:
            search_started = True
        rowids.append(cell[1])
        c_idx = first_idx+1
        while c_idx < len(cell_ptrs):
            cptr = cell_ptrs[c_idx]
            cell = parse_ICell(cptr,page)
            if cell[0] == col_val:
                rowids.append(cell[1])
                c_idx += 1
            else:
                searching = False
                break
        del page
    return rowids
            
if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2))
        database_file.seek(103)
        table_amt = int.from_bytes(database_file.read(2))
        print(f"database page size: {page_size}\nnumber of tables: {table_amt}")
elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2))
        page = read_page(database_file,1,page_size)
        cell_amt = read_int(page,103,2)
        cell_ptrs = [read_int(page,100+i,2) for i in range(8,8+(cell_amt<<1),2)]
        db_objs = get_db_schema(page,cell_ptrs)
        tbl_names = list(db_objs["tables"].keys())
        print(*tbl_names)
elif command.lower().startswith("select"):
    p_query = sp.parse(command.lower())
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2))
        page = read_page(database_file,1,page_size)
        
        cell_amt = read_int(page,103,2)
        cell_ptrs = [read_int(page,100+i,2) for i in range(8,8+(cell_amt<<1),2)]
        
        db_objs = get_db_schema(page,cell_ptrs)
        del page
        req_tbl_query = db_objs["tables"][p_query.table]["query"]
        records = []
        if p_query.cond and (index := get_valid_index(db_objs["indexes"],p_query.table,p_query.cond.col)):
            print("Have query and an index")
            rowids = travel_idxs(p_query.cond,index["pg_num"],database_file,page_size)
            rowids.sort()
            #records = [get_record_by_id(rid) for rid in rowids]
            for rid in rowids:
                print("Rowid:",rid)
        else:     
            print("No query or index")
            page_num = db_objs["tables"][p_query.table]["pg_num"]
            tbl_info = db_objs["tables"][p_query.table]["query"]
            records = travel_pages(page_num,database_file,page_size,tbl_info,p_query)
            if p_query.count_cols:
                print(len(records))
            else:
                col_idxs = []
                for col in p_query.col_names:
                    col_idxs.append(tbl_info["desc"].col_names.index(col))
                results = [[r[col_idx] for col_idx in col_idxs] for r in records if r]
                for res in results:
                    print(*res,sep="|")
else:
    print(f"Invalid command: {command}")
