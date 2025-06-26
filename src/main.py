# Importa os módulos necessários
import sys
import subprocess
import os

# Função que executa um script Python e trata o resultado
def run_script(script_path, name):
    print(f"\n Iniciando: {name}")  

    # Executa o script Python utilizando o mesmo interpretador do sistema
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)

    # Se o script gerou alguma saída no terminal (stdout), imprime
    if result.stdout:
        print(result.stdout)

    # Verifica se o script teve erro (código de retorno diferente de 0)
    if result.returncode != 0:
        print(f" Falha ao executar: {name}")  # Exibe que houve erro nessa etapa
        if result.stderr:
            print("Detalhes do erro:")       # Se houver mensagens de erro, imprime
            print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)          # Interrompe a execução do pipeline

    # Se o script rodou sem erros, imprime mensagem de sucesso
    print(f"Etapa concluída com sucesso: {name}")

# Bloco principal que define a ordem dos scripts
if __name__ == "__main__":
    scripts = [        
        ("ingest_raw.py", "Ingest Raw"),             
        ("transform_bronze.py", "Transform Bronze"), 
        ("transform_silver.py", "Transform Silver")  
    ]

    # Executa cada script em ordem
    for path, name in scripts:
        run_script(path, name)

print("\n Pipeline finalizado com sucesso!")
