import os
import sys
import json
import random
import time
from datetime import datetime
from threading import Thread, Lock
from multiprocessing import Process, Queue

# =======================================================
# MOCK DA API E UTILITÁRIOS
# =======================================================

def mock_api(id_num):
    # Simula a latencia da rede
    time.sleep(random.uniform(0.01, 0.04))
    
    val = round(random.uniform(100.0, 999.0), 2)
    resp = {
        "id": int(id_num),
        "status": "ok",
        "valor": val
    }
    return json.dumps(resp)

def gerar_lista_ids(total_ids, nome_arq="lista_ids.txt"):
    with open(nome_arq, "w") as f:
        for i in range(1, total_ids + 1):
            f.write(f"{1000 + i}\n")

# =======================================================
# PROCESSO P1 (TRABALHADOR / THREADS)
# =======================================================

def processo_p1(num_threads):
    # Leitura do arquivo de IDs
    if not os.path.exists("lista_ids.txt"):
        sys.exit(1)
        
    with open("lista_ids.txt", "r") as f:
        ids_para_processar = [linha.strip() for linha in f if linha.strip()]

    # Locks para garantir exclusao mutua entre threads
    lock_fila = Lock()
    lock_log = Lock()
    
    # Limpa ou cria o arquivo de log para a execucao atual
    with open("log_execucao.txt", "w") as f:
        pass

    def worker(t_name):
        while True:
            # Exclusao mutua ao pegar um ID da lista
            with lock_fila:
                if not ids_para_processar:
                    break
                curr_id = ids_para_processar.pop(0)

            # Consulta mock da API
            json_res = mock_api(curr_id)
            dt_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Formatacao exigida no PDF: <data>, <thread_id>, <id_processado>, <resposta_json>
            linha_log = f"{dt_str}, {t_name}, {curr_id}, {json_res}\n"

            # Exclusao mutua ao escrever no log
            with lock_log:
                with open("log_execucao.txt", "a") as f:
                    f.write(linha_log)

    # Criacao e disparo das threads
    threads = []
    for i in range(num_threads):
        t = Thread(target=worker, args=(f"Thread-{i+1}",))
        threads.append(t)
        t.start()

    # Aguarda todas as threads finalizarem
    for t in threads:
        t.join()

    sys.exit(0)

# =======================================================
# PROCESSO P0 (ORQUESTRADOR)
# =======================================================

def orquestrar_execucao(n_threads):
    t_inicio = time.time()
    
    # Criacao de processo nativo compativel com qualquer OS
    proc_p1 = Process(target=processo_p1, args=(n_threads,))
    proc_p1.start()
    proc_p1.join()  # Aguarda o processo P1 finalizar (equivalente ao waitpid)
    
    t_fim = time.time()
    tempo_decorrido = t_fim - t_inicio
    exit_code = proc_p1.exitcode

    # Conferencia de integridade do log
    total_ids_orig = 0
    if os.path.exists("lista_ids.txt"):
        with open("lista_ids.txt", "r") as f:
            total_ids_orig = len([l for l in f if l.strip()])
            
    linhas_log = 0
    if os.path.exists("log_execucao.txt"):
        with open("log_execucao.txt", "r") as f:
            linhas_log = len([l for l in f if l.strip()])

    # Auditoria do termino
    if exit_code != 0:
        status_final = f"Erro (Code {exit_code})"
    elif linhas_log != total_ids_orig:
        status_final = "Enriquecimento Incompleto"
    else:
        status_final = "OK"

    return tempo_decorrido, status_final

def rodar_bateria_testes():
    # Tamanhos de listas solicitados no trabalho
    cenarios_lista = [
        ("Pequena", 20),
        ("Média", 100),
        ("Grande", 300)
    ]
    
    num_threads_paralelo = 4  # Quantidade N de threads escolhida para o teste
    resultados = []

    print("\nIniciando bateria de testes...\n")

    for nome_tam, qte in cenarios_lista:
        gerar_lista_ids(qte)
        
        # Teste Sequencial (N = 1)
        t_seq, st_seq = orquestrar_execucao(1)
        resultados.append((nome_tam, qte, 1, t_seq, st_seq))
        
        # Teste Concorrente (N > 1)
        t_par, st_par = orquestrar_execucao(num_threads_paralelo)
        resultados.append((nome_tam, qte, num_threads_paralelo, t_par, st_par))

    # Relatorio final em tabela no terminal
    print("\n" + "="*65)
    print("RELATÓRIO CONSOLIDADO DE DESEMPENHO")
    print("="*65)
    print(f"{'Tamanho':<10} | {'IDs':<6} | {'Threads':<8} | {'Tempo (s)':<10} | {'Status'}")
    print("-" * 65)
    for tam, qte, th, tm, st in resultados:
        print(f"{tam:<10} | {qte:<6} | {th:<8} | {tm:<10.2f} | {st}")
    print("="*65 + "\n")

if __name__ == "__main__":
    rodar_bateria_testes()