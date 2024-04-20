import sys
import app.sql_parser as sp

from dataclasses import dataclass

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

class PageType:
    InteriorIndex = 0x02
    InteriorTable = 0x05
    LeafIndex = 0x0a
    LeafTable = 0x0d

def read_int(file,size):
    return int.from_bytes(file.read(size), byteorder="big")

def read_varint(file):
    val = 0
    USE_NEXT_BYTE = 0x80
    BITS_TO_USE = 0x7F
    for _ in range(9):
        byte = read_int(file,1)
        val = (val << 7) | (byte & BITS_TO_USE)
        if byte & USE_NEXT_BYTE == 0:
            break
    return val

def read_varint_mem(buffer):
    val = 0
    buf_idx = 0
    USE_NEXT_BYTE = 0x80
    BITS_TO_USE = 0x7F
    for _ in range(9):
        byte = buffer[buf_idx]
        val = (val << 7) | (byte & BITS_TO_USE)
        if byte & USE_NEXT_BYTE == 0:
            break
        buf_idx += 1
    return val

def parse_record_body(srl_type,file):
    if srl_type == 0:
        return None
    elif srl_type == 1:
        return read_int(file,1)
    elif srl_type == 2:
        return read_int(file,2)
    elif srl_type == 3:
        return read_int(file,3)    
    elif srl_type == 4:
        return read_int(file,4)    
    elif srl_type == 5:
        return read_int(file,6)    
    elif srl_type == 6:
        return read_int(file,8)
    elif srl_type >= 12 and srl_type%2==0:
        datalen = (srl_type-12)//2
        return file.read(datalen).decode()
    elif srl_type >= 13 and srl_type%2==1:
        datalen = (srl_type-13)//2
        try:
            data = file.read(datalen) #.decode()
            return data.decode()
        except UnicodeDecodeError:
            print("SRL_TYPE:",srl_type)
            print("File pos:",hex(file.tell()),data,file=sys.stderr)
    else:
        print("INVALID SERIAL TYPE")
        return None
    
def parse_cell(c_ptr,file):
    file.seek(c_ptr)
    payload_size = read_varint(file)
    row_id = read_varint(file)
    format_hdr_start = file.tell()
    format_hdr_sz = read_varint(file)
    serial_types = []
    format_body_start = format_hdr_start+format_hdr_sz
    while file.tell() < format_body_start:
        serial_types.append(read_varint(file))
    record = []
    for srl_type in serial_types:
        record.append(parse_record_body(srl_type,file))
    return record

def get_table_info(cell_ptrs,dbfile,tbl_name):
    for cell_ptr in cell_ptrs:
        record = parse_cell(cell_ptr,dbfile)
        if record[1] == tbl_name:
            return {"rootpage":record[3],"desc":sp.parse(record[4].lower().replace("(","( ").replace(")"," )").replace(",",", "))}
        
def get_records(start_offset,cells,db_file,tdesc,query_ref):
    records = []
    for c_ptr in cells:
        cell = parse_cell(start_offset+c_ptr,db_file)
        record = {}
        for col_name, col_value in zip(tdesc.col_names,cell):
            record[col_name] = col_value
        if query_ref.cond and query_ref.cond.col in record.keys():
            if query_ref.cond.comp(record[query_ref.cond.col]):
                continue
        records.append(list(record.values()))
    return records

def travel_pages(pg_num,pgsz,db_file,tdesc,query_ref):
    db_file.seek(pg_num)
    page_type = read_int(db_file,1)
    db_file.seek(pg_num+3)
    cell_amt = read_int(db_file,2)
    db_file.seek(page_offset + (12 if page_type&8 == 0 else 8))
    cell_ptrs = [read_int(db_file,2) for _ in range(cell_amt)]
    if page_type == PageType.InteriorTable:
        records = []
        for c_ptr in cell_ptrs:
            db_file.seek(pg_num+c_ptr)
            page_num = read_int(db_file,4)
            key = read_varint(db_file)
            records.extend(travel_pages((page_num-1)*pgsz,pgsz,db_file,tdesc,query_ref))
        return records
    elif page_type == PageType.LeafTable:
        return get_records(pg_num,cell_ptrs,db_file,tdesc,query_ref)
            
if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        database_file.seek(103)
        table_amt = int.from_bytes(database_file.read(2), byteorder="big")
        print(f"database page size: {page_size}\nnumber of tables: {table_amt}")
elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        database_file.seek(103)
        cell_amt = read_int(database_file,2)
        database_file.seek(108)
        cell_ptrs = [read_int(database_file,2) for _ in range(cell_amt)]
        records = [parse_cell(cell_ptr,database_file) for cell_ptr in cell_ptrs]
        tbl_names = [rcd[2] for rcd in records if rcd[2] != "sqlite_sequence"]
        print(*tbl_names)
elif command.lower().startswith("select"):
    p_query = sp.parse(command.lower())
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        database_file.seek(103)
        cell_amt = read_int(database_file,2)
        database_file.seek(108)
        cell_ptrs = [read_int(database_file,2) for _ in range(cell_amt)]
        tbl_info = get_table_info(cell_ptrs,database_file,p_query.table)
        page_offset = (tbl_info["rootpage"]-1)*page_size
        records = travel_pages(page_offset,page_size,database_file,tbl_info["desc"],p_query)
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
