import sys
import app.sql_parser as sp

from dataclasses import dataclass

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

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
        return file.read(datalen).decode()
    else:
        print("INVALID SERIAL TYPE")
        return None
    
def parse_cell(c_ptr,file):
    database_file.seek(c_ptr)
    payload_size = read_varint(file)
    row_id = read_varint(file)
    format_hdr_start = file.tell()
    format_hdr_sz = read_varint(file)
    serial_types = []
    format_body_start = format_hdr_start+format_hdr_sz
    while file.tell() < format_body_start:
        serial_types.append(read_varint(file))
    records = []
    for srl_type in serial_types:
        records.append(parse_record_body(srl_type,file))
    return records
    

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
        records = [parse_cell(cell_ptr,database_file) for cell_ptr in cell_ptrs]
        tbl_info = {rcd[3:] for rcd in records if rcd[2] == p_query.table}
        tbl_rtpage = tbl_info[0]
        mktbl_query = sp.parse(tbl_info[1].lower())
        
        page_offset = (tbl_rtpage-1)*page_size
        database_file.seek(page_offset+3)
        cell_amt = read_int(database_file,2)
        database_file.seek(page_offset+8)
        cell_ptrs = [read_int(database_file,2) for _ in range(cell_amt)]
        records = [parse_cell(cell_ptr,database_file) for cell_ptr in cell_ptrs]
        col_name = p_query.col_names[0]
        col_idx = mktbl_query.index(col_name)
        
        results = [r[col_idx] for r in records]
        for res in results:
            print(res)
else:
    print(f"Invalid command: {command}")
