import pandas as pd
import random
import datetime
import os

# Parâmetros
n = 200
start_date = datetime.date(2024, 1, 1)
end_date = datetime.date(2024, 12, 31)
departments = ["RH", "TI", "Financeiro", "Marketing", "Operações"]

def random_date(start, end):
    delta = (end - start).days
    return start + datetime.timedelta(days=random.randint(0, delta))

data = []
for i in range(n):
    eid = 1000 + i
    vac_start = random_date(start_date, end_date - datetime.timedelta(days=15))
    vac_end = vac_start + datetime.timedelta(days=random.randint(5, 15))
    absences = random.randint(0, 5)
    status = "ativo" if random.random() > 0.05 else "inativo"
    data.append({
        "employee_id": eid,
        "employee_name": f"Emp{eid}",
        "department": random.choice(departments),
        "vacation_start": vac_start.isoformat(),
        "vacation_end": vac_end.isoformat(),
        "absences": absences,
        "status": status
    })

df = pd.DataFrame(data)

# Caminho absoluto para garantir que salva sempre na pasta correta
base_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(base_dir, "input_large.csv")

# Garante que a pasta existe
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Gravação
df.to_csv(output_path, index=False)
print(f"✅ CSV gerado com sucesso em {output_path}")
