import pandas as pd
import sqlite3
import glob
import os
import logging
import sys

def csv_to_sqlite(input_path=None, db_path=None, table_name='patrimonios'):
    logging.basicConfig(level=logging.INFO)
    
    if getattr(sys, 'frozen', False):
        dir_path = os.path.dirname(sys.executable)
    else:
        dir_path = os.path.dirname(os.path.abspath(__file__))
        
    if input_path is None:
        csv_file = glob.glob(os.path.join(dir_path, "*.csv"))
        xlsx_files = glob.glob(os.path.join(dir_path, "*.xlsx"))
        
        all_files = csv_file + xlsx_files
        
        if not all_files:
            logging.error("Nenhum arquivo CSV ou XLSX encontrado no diretorio")
            return
        
        input_path = all_files[0]
        if len(all_files) > 1:
            logging.warning(f"Mais de um arquivo encontrados. Usando {input_path}")
            
    if db_path is None:
        db_path = os.path.join(dir_path, "patrimonios.db")
    elif os.path.isdir(db_path):
        db_path = os.path.join(db_path, "patrimonios.db")
        
    if not os.path.exists(input_path):
        logging.error(f"Arquivo não encontrado: {input_path}")
        return

    file_type = os.path.splitext(input_path)[1].lower()

    chunksize = 10000
    conn = sqlite3.connect(db_path)
    create_indices = True

    try:
        total_row = 0
        
        if file_type == '.csv':
            reader = pd.read_csv(input_path, chunksize=chunksize)
            logging.info(f"Lendo arquivo CSV: {input_path}")
        
        elif file_type == '.xlsx':
            logging.info(f"Procesando arquivo XLSX: {input_path}")
            df = pd.read_excel(input_path, engine='openpyxl')
            chunks = [df[i:i + chunksize] for i in range(0, df.shape[0], chunksize)]
            reader = iter(chunks)
        else:
            logging.error("Formato de arquivo não suportado. Use CSV ou XLSX.")
            return
        
        for i, chunk in enumerate(reader):
            if 'descricao' in chunk.columns:
                chunk['descricao_upper'] = chunk['descricao'].str.upper()
            
            if i == 0:
                chunk.to_sql(table_name, conn, if_exists='replace', index=False)
                logging.info(f"Tabela {table_name} criada. Importando dados...")
                
            else:
                chunk.to_sql(table_name, conn, if_exists='append', index=False)
            
            total_row += len(chunk)
            logging.info(f"Importados {total_row} registros...")
            
        if create_indices:
            logging.info("Criando indices...")
            cur = conn.cursor()
            
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_codigo ON {table_name}(codigo)")
        
            try:
                cur.execute(f"""
                            CREATE VIRTUAL TABLE IF NOT EXISTS {table_name}_fts USING fts5(
                            descricao_upper,
                            content={table_name},
                            content_rowid=rowid,
                            tokenize='porter unicode61'
                            )
                """)
                
                cur.execute(f"""
                            INSERT INTO {table_name}_fts({table_name}_fts) VALUES('rebuild')
                """)
                logging.info("Indices criados com sucesso.")
                
            except sqlite3.OperationalError as e:
                logging.warning(f"Aviso: Índice FTS não criado: {e}")
                logging.info("Usando índice convencional para texto")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_descricao_upper ON {table_name}(descricao_upper)")
                
        conn.commit()
        logging.info("Importação concluída.")
        
        db_size = os.path.getsize(db_path) / (1024 * 1024)
        logging.info(f"Tamanho do banco de dados: {db_size:.2f} MB")
        logging.info(f"Total de registros importados: {total_row}")
    
    except Exception as e:
        logging.error(f"Erro durante a importação: {e}")
        conn.rollback()
        
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        input_path = None
        
    if len(sys.argv) > 2:
        db_path = sys.argv[2]
    else:
        db_path = None
        
    csv_to_sqlite(input_path, db_path)