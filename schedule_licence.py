import subprocess
import sys
import os

param = sys.argv[1]

def run_script(script_name, param=None):
    try:
        if param:
            result = subprocess.run(['python3', script_name, '--param', param], check=True)
        else:
            result = subprocess.run(['python3', script_name], check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"{script_name} failed with exit code {e.returncode}")
        return e.returncode

def main():
    
    # Run the first script
    first_script = param+'.py'
    first_script_path = os.path.join('/var/rel8ed.to/nfs/share/project_originals/scripts/', first_script)
    print('Running the cooking script:', first_script_path)
    exit_code = run_script(first_script_path)
    
    # Check if the first script ran successfully
    if exit_code == 0:
        # Run the second script
        run_script('/home/rli/ingestion_scripts/licence_sql.py', param)
        print('Running the loading script')
    else:
        print("First script failed. Second script will not run.")

if __name__ == "__main__":
    main()
