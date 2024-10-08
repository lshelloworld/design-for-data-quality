# -*- coding: utf-8 -*-
"""
Created on Mon Jan 09 10:31:36 2023

"""

import pyodbc
import random
import time

conn_str = "DRIVER={SQL Server};SERVER=127.0.0.1;DATABASE=Update2021"

def clear_cache(conn, cursor):
    cursor.execute("DBCC FREESYSTEMCACHE('ALL')")
    cursor.execute("DBCC FREESESSIONCACHE")
    cursor.execute("DBCC FREEPROCCACHE")
    conn.commit()

def find_update_value(conn, cursor, tb_name, v):
    values = None
    try:

        cols = ','.join(v)
        sql_stmt = f"SELECT {cols}, COUNT(*) AS M FROM {tb_name} GROUP BY {cols} ORDER BY M DESC"
        cursor.execute(sql_stmt)
        values = cursor.fetchone()
        conn.commit()
        
    except Exception as e:
        print(sql_stmt)
        print("Find update value", e)
        
    return values

def find_query_value(v):
    values = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        cols = ','.join(v)
        sql_stmt = f"SELECT {cols}, COUNT(*) AS M FROM R GROUP BY {cols} ORDER BY M DESC"
        cursor.execute(sql_stmt)
        values = cursor.fetchone()
        conn.commit()
        
    except Exception as e:
        print(sql_stmt)
        print("Find update value", e)
        
    return values

def run_join_query(tb_name1, tb_name2, join_cols, v, m):
    conn = None
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        clear_cache(conn, cursor)
        
        join_con = " AND ".join([f'T1.{c}=T2.{c}' for c in join_cols])
        where_clause = " AND ".join([f"T1.{k}='{m[i]}'" for i, k in enumerate(v)])
        sql_stmt = f"SELECT * FROM {tb_name1} T1 JOIN {tb_name2} T2 ON {join_con} WHERE {where_clause} OPTION (MAXDOP 1)"
        cursor.execute(sql_stmt)
        for _ in cursor.fetchall():
            pass
        conn.commit()
    except Exception as e:
        print(sql_stmt)
        print("run_join_query:", e)
        
def run_view_query(view_name, v, m):
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        clear_cache(conn, cursor)
   
        where_clause = " AND ".join([f"{k}='{m[i]}'" for i, k in enumerate(v)])
        sql_stmt = f"SELECT * FROM {view_name} WHERE {where_clause} OPTION (MAXDOP 1)"
        cursor.execute(sql_stmt)
        for _ in cursor.fetchall():
            pass
        conn.commit()
    except Exception as e:
        print(sql_stmt)
        print("run_join_query:", e)


def run_join_update(tb_name1, tb_name2, join_cols, join_proj_cols, c_fds, u_fds, propagate = True):
    conn = None
    join_con = ' AND '.join([f'T1.{c}=T2.{c}' for c in join_cols])
    join_proj = ",".join(join_proj_cols)
    try:
        conn  = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        #Check FD in the view (single RHS attribute)
        for fd in c_fds:
            lhs_str = ",".join([f"T.{c}" for c in fd['lhs']])
            rhs_a = "T."+fd['rhs'][0]
            sql_stmt = f'SELECT {lhs_str}, COUNT({rhs_a}) AS C FROM (SELECT {join_proj} FROM {tb_name1} T1 JOIN {tb_name2} T2 ON {join_con}) T GROUP BY {lhs_str} HAVING COUNT({rhs_a}) > 1 OPTION (MAXDOP 1)'
            cursor.execute(sql_stmt)
            for _ in cursor.fetchall():
                pass
            cursor.commit()
        
        for u_fd in u_fds:
            m = find_update_value(conn, cursor, 'R', u_fd['lhs'])
            update_con = " AND ".join([f'{c}={m[i]}' for i, c in enumerate(u_fd['lhs'])])
            #Update values in the table
            sql_stmt = f"UPDATE R SET {u_fd['rhs'][0]}='{str(round(random.random(),10))}' WHERE {update_con} OPTION	(MAXDOP 1)"
            cursor.execute(sql_stmt)
            if propagate:
                sql_stmt = f"UPDATE R SET {u_fd['rhs'][0]}='{str(round(random.random(),10))}' WHERE {update_con} OPTION	(MAXDOP 1)"
                cursor.execute(sql_stmt)
            conn.rollback()
    except Exception as e:
        print(sql_stmt)
        print("Run join update", e)
        

def run_view_update(view_name, tb_name, c_fds, u_fds, propagate=True):
    conn = None
    try:
        conn  = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        #Check FD in the view (single RHS attribute)
        for fd in c_fds:
            lhs_str = ",".join(fd['lhs'])
            rhs_a = fd['rhs'][0]
            sql_stmt = f'SELECT {lhs_str}, COUNT({rhs_a}) AS C FROM {view_name} GROUP BY {lhs_str} HAVING COUNT({rhs_a}) > 1 OPTION	(MAXDOP 1)'
            cursor.execute(sql_stmt)
            for _ in cursor.fetchall():
                pass
            cursor.commit()
        for u_fd in u_fds:
            m = find_update_value(conn, cursor, tb_name, u_fd['lhs'])
            update_con = " AND ".join([f'{c}={m[i]}' for i, c in enumerate(u_fd['lhs'])])
            #Update values in the table
            sql_stmt = f"UPDATE {tb_name} SET {u_fd['rhs'][0]}='{str(round(random.random(),10))}' WHERE {update_con} OPTION	(MAXDOP 1)"
            cursor.execute(sql_stmt)
            conn.rollback()
    
        if propagate:
            for u_fd in u_fds:
                m = find_update_value(conn, cursor, view_name, u_fd['lhs'])
                update_con = " AND ".join([f'{c}={m[i]}' for i, c in enumerate(u_fd['lhs'])])
                #Update values in the table
                sql_stmt = f"UPDATE {view_name} SET {u_fd['rhs'][0]}='{str(round(random.random(),10))}' WHERE {update_con} OPTION (MAXDOP 1)"
                cursor.execute(sql_stmt)
                conn.rollback()
    except Exception as e:
        print(sql_stmt)
        print("View update", e)
        
        
def create_table_stmt(tb_name, columns, keys):
    col_defs = ','.join([f'{c} varchar(100) NOT NULL' for c in columns])
    key_defs = ",".join([f"CONSTRAINT {tb_name}_UC_{k} UNIQUE({','.join([c for c in v])})" for k, v in enumerate(keys)])
    create_table_stmt = f"CREATE TABLE {tb_name} ({col_defs},{key_defs})"
    
    return create_table_stmt

def gen_init_table(tb_name, columns, cards, keys):
    conn = None
    
    try:
        print("Initializing R")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        row_counts = {a:0 for a in columns}
        
        cursor.execute(create_table_stmt(tb_name, columns, keys))
        conn.commit()
        
        for v, max_card in cards:
            for _ in range(max_card):
                #Insert a value
                value_str = ','.join([str(row_counts[a]) for a in columns])
                cursor.execute(f'INSERT INTO {tb_name} VALUES ({value_str})')
                for a in [a for a in columns if a not in v]:
                        row_counts[a]+=1
            for a in v:
                row_counts[a]+=1
        conn.commit()
        conn.close()
    except Exception as e:
        print("gen_init_table:", e)
        if conn:
            conn.close()            
    
def populate_table(f_table, t_table, columns, keys):
    conn= None
    try:
        print("Populating ", t_table)
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        col_str = ','.join(columns)
        
        cursor.execute(create_table_stmt(t_table, columns, keys))
        conn.commit()
        
        cursor.execute(f"INSERT INTO {t_table} SELECT DISTINCT {col_str} FROM {f_table}")
        conn.commit()
        
    except Exception as e:
        print("Populate table", e)
        if conn:
            conn.close()
            
def prepare_env():
    columns = ['e','c','v','t']
    keys = [['v', 't'], ['e', 't'], ['c', 't']]
    cards = [(['e', 'c'], 1000), (['c', 'v'], 100)]
    gen_init_table("R", columns, cards, keys)
    
    populate_table("R", "R1", ['e', 'c'], [['e']])
    populate_table("R", "R2", ['v', 'c'], [['v']])
    populate_table("R", "R3", ['v', 't', 'e'], [['v', 't'], ['e', 't']])
    populate_table("R", "R4", ['c', 'e', 't'], [['c', 't'], ['e', 't']])
    populate_table("R", "R5", ['c', 't', 'v'], [['c', 't'], ['v', 't']])
    
    
def exp_schemata_R(count=10):
    print("Schemata R")
#    count = 10
 
    update_time_e = time.time()
    for _ in range(count):
        run_view_update("R", "R", [{'lhs':['e'], 'rhs':['c']}], [{'lhs':['e'], 'rhs':['c']}], propagate=False)
    update_time_e = (time.time() - update_time_e) / count
    
    update_time_v = time.time()
    for _ in range(count): #Need to propogate to the view when update R2
        run_view_update("R", "R", [{'lhs':['v'], 'rhs':['c']}], [{'lhs':['v'], 'rhs':['c']}], propagate=False)
    update_time_v = (time.time() - update_time_v) / count
#    #############################################################
#    
    m_v = find_query_value(['v'])
    m_e = find_query_value(['e'])
    
        
    query_time_e = time.time()
    for _ in range(count):
        run_view_query("R", ['e'], m_e)
    query_time_e = (time.time() - query_time_e) / count

    
    query_time_v = time.time()
    for _ in range(count):
        run_view_query("R", ['v'], m_v)
    query_time_v = (time.time() - query_time_v) / count
    
    print("Update C based on E:", update_time_e)
    print("Update C based on V:", update_time_v)
    print("Query CT based on E:", query_time_e)
    print("Query CT based on V:", query_time_v)
    
    
def exp_schemata_D(count=10):
    print("Schemata D")
#    count = 10
 
    update_time_e = time.time()
    for _ in range(count):
        run_view_update("R", "R4", [{'lhs':['e'], 'rhs':['c']}], [{'lhs':['e'], 'rhs':['c']}], propagate=False)
    update_time_e = (time.time() - update_time_e) / count
    
    update_time_v_p = time.time()
    for _ in range(count): #Need to propogate to the view when update R2
        run_view_update("R", "R2", [{'lhs':['v'], 'rhs':['c']}], [{'lhs':['v'], 'rhs':['c']}])
    update_time_v_p = (time.time() - update_time_v_p) / count
    
    update_time_v = time.time()
    for _ in range(count): #No Need to propogate to the view when update R2
        run_view_update("R", "R2", [{'lhs':['v'], 'rhs':['c']}], [{'lhs':['v'], 'rhs':['c']}], propagate=False)
    update_time_v = (time.time() - update_time_v) / count
#    
#    #############################################################
#    
    m_v = find_query_value(['v'])
    m_e = find_query_value(['e'])
#    
#        
    query_time_e = time.time()
    for _ in range(count):
        run_view_query("R4", ['e'], m_e)
    query_time_e = (time.time() - query_time_e) / count

    
    query_time_v_join = time.time()
    for _ in range(count):
        run_join_query("R2", "R3", ['v'], ['v'], m_v)
    query_time_v_join = (time.time() - query_time_v_join)/10
    
    query_time_v = time.time()
    for _ in range(count):
        run_view_query("R", ['v'], m_v)
    query_time_v = (time.time() - query_time_v) / count
    
    print("Update C based on E:", update_time_e, "\t", update_time_e)
    print("Update C based on V:", update_time_v, "\t", update_time_v_p)
    print("Query CT based on E:", query_time_e, "\t", query_time_e)
    print("Query CT based on V:", query_time_v_join, "\t",query_time_v)
   
    
    
        
def exp_schemata_D_p(count=10):
    print("Schemata D'")
#    count = 10
 
    update_time_e = time.time()
    for _ in range(count):
        run_view_update("R", "R1", [{'lhs':['e'], 'rhs':['c']}], [{'lhs':['e'], 'rhs':['c']}], propagate=False)
    update_time_e = (time.time() - update_time_e) / count
    
    update_time_e_p = time.time()
    for _ in range(count):
        run_view_update("R", "R1", [{'lhs':['e'], 'rhs':['c']}], [{'lhs':['e'], 'rhs':['c']}])
    update_time_e_p = (time.time() - update_time_e_p) / count
    
    update_time_v = time.time()
    for _ in range(count): #Need to propogate to the view when update R2
        run_view_update("R", "R5", [{'lhs':['v'], 'rhs':['c']}], [{'lhs':['v'], 'rhs':['c']}], propagate=False)
    update_time_v = (time.time() - update_time_v) / count
#    #############################################################
#    
    m_v = find_query_value(['v'])
    m_e = find_query_value(['e'])
#    
#    
    query_time_e_join = time.time()
    for _ in range(count):
        run_join_query("R1", "R3", ['e'], ['e'], m_e)
    query_time_e_join = (time.time() - query_time_e_join) / count
        
    query_time_e = time.time()
    for _ in range(count):
        run_view_query("R", ['e'], m_e)
    query_time_e = (time.time() - query_time_e) / count

    
    query_time_v = time.time()
    for _ in range(count):
        run_view_query("R5", ['v'], m_v)
    query_time_v = (time.time() - query_time_v) / count
    
    print("Update C based on E:", update_time_e, "\t", update_time_e_p)
    print("Update C based on V:", update_time_v, "\t", update_time_v)
    print("Query CT based on E:", query_time_e_join, "\t", query_time_e)
    print("Query CT based on V:", query_time_v, "\t", query_time_v)
    
   
def exp_schemata_D_g(count=10):
    print("Schemata Dg")
#    count = 10
 
    update_time_e = time.time()
    for _ in range(count):#Directly update view
        run_join_update("R2", "R3", ['v'], ['T1.v', 'T1.c', 'T2.t', 'T2.e'], [{'lhs':['v'], 'rhs':['c']}, {'lhs':['c', 't'], 'rhs':['v']}], [{'lhs':['e'], 'rhs':['c']}], propagate=False)
    update_time_e = (time.time() - update_time_e) / count
    
    update_time_e_p = time.time()
    for _ in range(count):#Directly update view
        run_join_update("R2", "R3", ['v'], ['T1.v', 'T1.c', 'T2.t', 'T2.e'], [{'lhs':['v'], 'rhs':['c']}, {'lhs':['c', 't'], 'rhs':['v']}], [{'lhs':['e'], 'rhs':['c']}])
    update_time_e_p = (time.time() - update_time_e_p) / count
    
    update_time_v = time.time()
    for _ in range(count): #Need to propogate to the view when update R2
        run_view_update("R", "R2", [{'lhs':['v'], 'rhs':['c']}], [{'lhs':['v'], 'rhs':['c']}], propagate=False)
    update_time_v = (time.time() - update_time_v) / count
    #############################################################
    
    m_v = find_query_value(['v'])
    m_e = find_query_value(['e'])
    
     
    query_time_e_join = time.time()
    for _ in range(count):
        run_join_query("R3", "R2", ['v'], ['e'], m_e)
    query_time_e_join = time.time() - query_time_e_join
        
    query_time_e = time.time()
    for _ in range(count):
        run_view_query("R", ['e'], m_e)
    query_time_e = (time.time() - query_time_e) / count

    
    query_time_v = time.time()
    for _ in range(count):
        run_join_query("R2", "R3", ['v'], ['v'], m_v)
    query_time_v = (time.time() - query_time_v) / count
    
    print("Update C based on E:", update_time_e, "\t", update_time_e_p)
    print("Update C based on V:", update_time_v, "\t", update_time_v)
    print("Query CT based on E:", query_time_e_join, "\t", query_time_e)
    print("Query CT based on V:", query_time_v, "\t", query_time_v)    
    
def exp_schemata_D_h(count=10):
    print("Schemata Dh")
#    count = 10
 
    update_time_e = time.time()
    for _ in range(count): #Need to propogate to the view
        run_view_update("R", "R1", [{'lhs':['e'], 'rhs':['c']}], [{'lhs':['e'], 'rhs':['c']}], propagate=False)
    update_time_e = (time.time() - update_time_e) / count

    update_time_v = time.time()
    for _ in range(count): #Need to propogate to the view when update R2
        run_view_update("R", "R2", [{'lhs':['v'], 'rhs':['c']}], [{'lhs':['v'], 'rhs':['c']}], propagate=False)
    update_time_v = (time.time() - update_time_v) / count

    update_time_v_p = time.time()
    for _ in range(count):#Directly update view
        run_join_update("R1", "R3", ['e'], ['T1.e', 'T1.c', 'T2.v', 'T2.t'], [{'lhs':['e'], 'rhs':['c']}, {'lhs':['v', 't'], 'rhs':['e']}, {'lhs':['e', 't'], 'rhs':['v']}], [{'lhs':['v'], 'rhs':['c']}], propagate=True)
    update_time_v_p = (time.time() - update_time_v_p) / count
#    #############################################################
#    
    m_v = find_query_value(['v'])
    m_e = find_query_value(['e'])
    
        
    query_time_e = time.time()
    for _ in range(count):
         run_join_query("R1", "R3", ['e'], ['e'], m_e)
    query_time_e = (time.time() - query_time_e) / count

    query_time_v_join = time.time()
    for _ in range(count):
        run_join_query("R3", "R1", ['e'], ['v'], m_v)
    query_time_v_join = (time.time() - query_time_v_join) / count

    
    query_time_v = time.time()
    for _ in range(count):
        run_view_query("R", ['v'], m_v)
    query_time_v = (time.time() - query_time_v) / count
    
    print("Update C based on E:", update_time_e, "\t", update_time_e)
    print("Update C based on V:", update_time_v, "\t", update_time_v_p)
    print("Query CT based on E:", query_time_e, "\t", query_time_e)
    print("Query CT based on V:", query_time_v_join,"\t", query_time_v)    
   
    
    
    
if __name__ == '__main__':
    
    c = 100
    
    fds = []
    fds.append({'lhs':['e'], 'rhs':['c']})
    fds.append({'lhs':['v'], 'rhs':['c']})
    fds.append({'lhs':['v', 't'], 'rhs':['e']})
    fds.append({'lhs':['e', 't'], 'rhs':['v']})
    fds.append({'lhs':['c', 't'], 'rhs':['v']})
    
#    prepare_env()    
    exp_schemata_R(count=c)
    exp_schemata_D(count=c)
    exp_schemata_D_p(count=c)
    exp_schemata_D_g(count=c)
    exp_schemata_D_h(count=c)
