# Initial Requirements
## Install Chocolatey to automate installation
1. Open a Powershell terminal ***as Administrator***
2. Paste and Execute:
    ```
    Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
    ```
3. Restart Terminal ***as Administrator***
## Install Prerequisites
1. Install Pyenv-win
    ```
    choco install pyenv-win -y
    ```
2. Install PostgreSQL and pinning version to mitigate accidental upgrades
    ```
    choco install postgresql17 -y --params '/Password:postgres /Port:5432'
    choco pin add -n postgresql17
    ```
3. Install Git and GitHub-CLI
    ```
    choco install git gh -y
    ```
4. Install Python **3.12.6** and pinning version to mitigate accidental upgrades
    ```
    choco install python --version=3.12.6
    choco pin add -n python
    ```
# Setting Up Applications
## PostgreSQL
1. Login as user `postgres` with password `postgres`
    ``` 
    psql -U postgres
    ```
    Within the postgres CLI, execute the next few commands
2. Create new user `credpulse`
    ```
    CREATE USER credpulse WITH PASSWORD 'credpulse';
    ```
3. Create new databases `credpulse` and `credpulse_test`
    ```
    CREATE DATABASE credpulse OWNER credpulse;
    CREATE DATABASE credpulse_test OWNER credpulse;
    ```
4. Close PostgreSQL CLI
    ```
    exit
    ```
With this, we now have a PostgreSQL server and database setup.
Details at glance:
- Username: credpulse
- Main Database: credpulse
- Testing Database: credpulse_test
- Password: credpulse

## Git and GitHub
1. Setup git username and email. Make sure to ***replace details*** in these commands
    ```
    git config --global user.name "Your Name"
    git config --global user.email "your.email@example.com"
    ```
2. Authenticate with GitHub account
    ```
    gh auth login
    gh auth status
    ```

## Project and Development Environment
1. Clone Repository *(Do not do this in a cloud storage. Choose only a local folder.)*
    ```
    git clone https://github.com/rahulb-scg/credpulse-backend.git
    cd credpulse-backend
    ```
2. Create and activate the virtual environment
    ```
    python -m venv venv
    .\venv\Scripts\activate
    ```
3. Project Dependecies
    ```
    pip install -r requirements.txt
    ```
4. Deactivate the virtual environment
    ```
    deactivate
    ```
5. Setup project testing files
    ```
    cd backend
<<<<<<< HEAD
=======
    python backend\prepareTestData.py
>>>>>>> cd77f6397c00cf40a19534269a42a67d28a112e4
    python prepareTestData.py
    ```
    On execution, this script will download testing data and outputs
