import os
from git import Repo

def validate_pull_request():
    # Chemin du repo (GitHub Actions place le repo dans le répertoire de travail)
    repo_path = os.getcwd()
    repo = Repo(repo_path)

    # Récupérer la branche de la PR et la branche de base
    diff = repo.head.commit.diff('origin/main')

    # Vérifier qu'aucun fichier n'a été supprimé
    deleted_files = [d.a_path for d in diff if d.deleted_file]
    if deleted_files:
        print(f"Échec : Des fichiers ont été supprimés dans la PR : {', '.join(deleted_files)}")
        exit(1)

    # Vérifier que utils.py n'a pas été modifié
    utils_changes = [d.a_path for d in diff if d.a_path == 'utils.py']
    if utils_changes:
        print("Échec : Des modifications ont été détectées dans utils.py.")
        exit(1)

    print("Validation réussie : Aucun fichier supprimé et utils.py inchangé.")
    exit(0)

if __name__ == "__main__":
    validate_pull_request()
