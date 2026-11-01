# Windows setup and GitHub publishing

## A. Install the required free tools

1. Install Python 3 from https://www.python.org/downloads/ . During setup, enable **Add Python to PATH**.
2. Install the Microsoft Python extension in VS Code.
3. Install Git from https://git-scm.com/downloads .
4. Create a free GitHub account at https://github.com/signup .
5. SSMS is only a database client. You can use your existing Azure SQL connection for the optional SQL scripts; the core Python demo does not require a SQL Server engine.

## B. Open and run the project

1. Extract the ZIP file.
2. In VS Code choose **File → Open Folder** and select `data_quality_monitor`.
3. Open Terminal → New Terminal.
4. Check Python and Git:

```powershell
py --version
git --version
```

5. Create and activate a virtual environment:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, use Command Prompt terminal and run:

```bat
.venv\Scripts\activate.bat
```

6. Install dependencies:

```powershell
py -m pip install -r requirements.txt
```

7. Run the baseline batch:

```powershell
$env:PYTHONPATH = "src"
py run.py --input data/input/batch_001.csv
```

8. Run the anomaly batch:

```powershell
py run.py --input data/input/batch_002_anomaly.csv
```

9. Run the late-arriving batch:

```powershell
py run.py --input data/input/batch_003_late.csv
```

10. Run tests:

```powershell
py -m pytest -q
```

Look in `reports/` for JSON run reports. The local database is created under `data/` when the pipeline runs.

## C. Inspect the optional SQL Server model in SSMS

1. Connect to your existing Azure SQL database or a local SQL Server database that you are authorized to use.
2. Create/use a dedicated empty project database. Do not run the DDL in a production database.
3. Open `sql/01_sqlserver_schema.sql` in SSMS and execute it.
4. Open `sql/02_sqlserver_useful_queries.sql` to see the inspection queries. They will return empty results until data is loaded into those SQL Server tables.

## D. Create a GitHub repository

1. Sign in to GitHub and create a **new public repository** named `yottaflex-data-quality-monitor`.
2. Do not initialize it with a README if this project already has one.
3. In the VS Code terminal, from the project root, run:

```powershell
git init
git add .
git status
git commit -m "Build data quality monitoring pipeline"
git branch -M main
git remote add origin https://github.com/YOUR_GITHUB_USERNAME/yottaflex-data-quality-monitor.git
git push -u origin main
```

Replace `YOUR_GITHUB_USERNAME` with your actual GitHub username. GitHub may ask you to authenticate in a browser.

## E. Before pushing

- Review `git status` and inspect the files being committed.
- Never commit passwords, access tokens, `.env` files, private data, or real customer records.
- Keep only synthetic/sample data in the public repository.
- Add your complete AI prompt history to `PROMPT_HISTORY.md` as you work. Include prompts submitted to every AI tool, as required by the candidate instructions. AI responses are optional. Do not claim prompts you did not actually use.
