import re

def getProjectId(file_path="terraform/variables.tf"):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        pattern = r'variable\s+"gcp_project_id"\s*\{[^}]*?default\s*=\s*"([^"]+)"'
        match = re.search(pattern, content, re.DOTALL)
        
        if match:
            return match.group(1)
        
        return match
        
    except FileNotFoundError:
        print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
        return None
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")
        return None