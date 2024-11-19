import subprocess
from mage_ai.settings.repo import get_repo_path

def execute_command(command):
    try:
        process = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        output, error = process.communicate()
        output = output.decode("utf-8")
        error = error.decode("utf-8")
        exit_code = process.returncode
        return output, error, exit_code
    except Exception as e:
        return None, str(e), -1

@custom
def remove_logs(*args, **kwargs):
    logger = kwargs.get('logger')
    project_path = get_repo_path()
    clean_logs = f"mage clean-old-logs {project_path}"
    output, error, exit_code = execute_command(clean_logs)
    logger.info(f"Clean logs outputs: {output}, {error}, {exit_code}")
    clean_cached_outputs = f"mage clean-cached-variables {project_path}"
    output_cached, error_cached, exit_cached = execute_command(clean_cached_outputs)
    logger.info(f"Clean cached outputs: {output_cached}, {error_cached}, {exit_cached}")
    return