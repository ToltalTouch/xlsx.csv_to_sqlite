import pandas as pd
import sqlite3
import glob
import os
import logging
import sys

def csv_to_sqlite(input_path=None, db_path=None, table_name=None, delimiter=None):
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configurar log para console e arquivo
    log_file = os.path.join(log_dir, 'importacao.log')
    
    # Configuração de logging para console e arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Mantém logs no console também
        ]
    )
    
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
        db_path = os.path.join(dir_path, table_name + ".sqlite")
    elif os.path.isdir(db_path):
        db_path = os.path.join(db_path, table_name + ".sqlite")

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
            if delimiter is None:
                with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
                    sample = f.read(4096)
                    if sample.count(';') > sample.count(','):
                        delimiter = ';'
                    else:
                        delimiter = ','
                        
            logging.info(f"Processando arquivo CSV: {input_path} com delimitador '{delimiter}'")
            
            encodings = ['utf-8', 'latin1', 'utf-16', 'cp1252']
            success = False

            for encoding in encodings:
                try:
                    reader = pd.read_csv(
                        input_path,
                        chunksize=chunksize,
                        delimiter=delimiter,
                        on_bad_lines='warn',
                        encoding=encoding,
                        dtype=str,
                        low_memory=False
                )
                    
                    next(reader)
                    reader = pd.read_csv(
                        input_path,
                        chunksize=chunksize,
                        delimiter=delimiter,
                        on_bad_lines='warn',
                        encoding=encoding,
                        dtype=str,
                        low_memory=False
                    )
                    logging.info(f"Arquivo CSV lido com sucesso usando codificação {encoding}")
                    
                    success = True
                    break
                except UnicodeDecodeError as e: 
                    logging.warning(f"Falha ao ler CSV com codificação {encoding}: {e}")

                except Exception as e:
                    logging.warning(f"Erro ao ler CSV com codificação {encoding}: {e}")
            if not success:
                logging.info("Tentando abordagem diferente")
                try:
                    reader = pd.read_csv(
                        input_path,
                        chunksize=chunksize,
                        delimiter=delimiter,
                        on_bad_lines='skip',
                        encoding='latin1',
                        quoting=3,
                        escapechar='\\',
                        dtype=str
                    )
                except Exception as e:
                    logging.error(f"Falha ao ler CSV com codificação latin1: {e}")
                    raise
            
        elif file_type == '.xlsx':
            logging.info(f"Processando arquivo XLSX: {input_path}")
            df = pd.read_excel(input_path, engine='openpyxl')
            chunks = [df[i:i + chunksize] for i in range(0, df.shape[0], chunksize)]
            reader = iter(chunks)
        else:
            logging.error(f"Formato não suportado: {file_type}")
            
        for i, chunk in enumerate(reader):
            if i ==0:
                chunk.to_sql(table_name, conn, if_exists='replace', index=False)
                logging.info(f"Tabela '{table_name}' criada e dados inseridos (primeiro chunk).")
            else:
                chunk.to_sql(table_name, conn, if_exists='append', index=False)
                logging.info(f"Chunk {i+1} inserido na tabela '{table_name}'.")
            
            logging.info(f"Linhas inseridas neste chunk: {len(chunk)}")
            
        if create_indices:
            logging.info("Criando índices para otimizar consultas...")
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table_name})")
            columns = [info[1] for info in cur.fetchall()]
            logging.info(f"Colunas na tabela: {', '.join(columns)}")

            if 'id' not in columns:
                try:
                    logging.info("Criando índice para a coluna 'id'...")
                    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN id INTEGER PRIMARY KEY AUTOINCREMENT")
                    cur.execute(f"UPDATE {table_name} SET id = NULL")
                    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name} (id)")
                    conn.commit()
                except Exception as e:
                    logging.error(f"Erro ao criar índice na coluna 'id': {e}")
                    
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
    
    table_name = input("Digite o nome da tabela para importar os dados ")
    csv_to_sqlite(input_path, db_path, table_name=table_name)