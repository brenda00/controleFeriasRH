# Controle de Férias RH

Este projeto implementa um pipeline ETL (Extract, Transform, Load) para controle de férias de funcionários, utilizando PySpark para processamento de dados em múltiplas camadas (raw, bronze, silver, gold). O pipeline é automatizado via GitHub Actions e inclui testes de qualidade de dados.

---

## Estrutura do Projeto

```
controleFeriasRH/
├── data/
│   ├── raw/
│   │   └── input_large.csv
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
│   ├── main.py
│   ├── ingest_raw.py
│   ├── transform_bronze.py
│   ├── transform_silver.py
│   └── data_quality.py
├── tests/
│   └── test_data_quality.py
├── requirements.txt
├── .gitignore
└── .github/
    └── workflows/
        └── etl.yml
```

---

## Pré-requisitos

- Python 3.10+
- [Java 8+](https://adoptopenjdk.net/) (necessário para PySpark)
- [pip](https://pip.pypa.io/en/stable/)
- (Opcional) [virtualenv](https://virtualenv.pypa.io/en/latest/) para ambiente isolado

---

## Instalação

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/seu-usuario/controleFeriasRH.git
   cd controleFeriasRH
   ```

2. **(Opcional) Crie e ative um ambiente virtual:**
   ```bash
   python -m venv venv
   # Windows:
   venv\Scripts\activate
   # Linux/Mac:
   source venv/bin/activate
   ```

3. **Instale as dependências:**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

---

## Execução do Pipeline

### 1. Prepare o arquivo de entrada

Coloque o arquivo `input_large.csv` com os dados brutos na pasta `data/raw/`.

### 2. Execute o pipeline ETL

A partir da raiz do projeto, rode:

```bash
python src/main.py
```

O pipeline executará as seguintes etapas, na ordem:
- Ingestão dos dados brutos (raw) para bronze (`ingest_raw.py`)
- Transformação bronze para silver (`transform_bronze.py`)
- Transformação silver para gold (`transform_silver.py`)

Os dados processados serão salvos nas respectivas pastas em `data/`.

---

## Testes

Os testes unitários estão na pasta `tests/`.

Para rodar os testes:

```bash
pytest tests/
```

---

## Integração Contínua

O projeto utiliza GitHub Actions para rodar o pipeline e os testes automaticamente a cada push ou pull request na branch `main`. O workflow está em `.github/workflows/etl.yml`.

---

## Observações

- Certifique-se de que o Java está instalado e configurado no PATH para o PySpark funcionar.
- Os caminhos dos arquivos são relativos à raiz do projeto.
- Para grandes volumes de dados, ajuste a configuração do Spark conforme necessário.

---
