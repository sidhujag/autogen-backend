import requests

from functions_and_agents_metadata import AuthAgent
from pathlib import Path
from metagpt.utils.git_repository import GitRepository
class RepositoryService:
    @staticmethod
    def _create_github_repository(token: str, name: str, description: str="", private: bool =False):
        """
        Create a new GitHub repository.

        :param token: Personal access token for the GitHub API
        :param name: Name of the repository
        :param description: Description of the repository
        :param private: Boolean indicating whether the repository is private
        """
        url = "https://api.github.com/user/repos"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        data = {
            "name": name,
            "description": description,
            "private": private
        }
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 201:
            return {"response": f"Repository ({name}) created successfully!"}
        elif response.status_code == 422:
            return {"response": f"Repository ({name}) already exists!"}
        else:
            return {"error": f"Failed to create repository: {response.json()}"}
    
    @staticmethod
    def _check_repo_exists(username: str, repo_name: str, token: str):
        """
        Check if a repository exists on GitHub.

        :param username: GitHub username
        :param repo_name: Repository name
        :param token: GitHub Personal Access Token
        :return: True if repository exists, False otherwise
        """
        url = f"https://api.github.com/repos/{username}/{repo_name}"
        headers = {"Authorization": f"token {token}"}
        response = requests.get(url, headers=headers)
        return response.status_code == 200

    @staticmethod
    def _fork_repository(token: str, repo_full_name: str):
        """
        Fork a repository on GitHub.

        :param token: GitHub Personal Access Token
        :param repo_full_name: Full name of the repository (e.g., "original-owner/repo")
        :return: URL of the forked repository, or error message
        """
        url = f"https://api.github.com/repos/{repo_full_name}/forks"
        headers = {"Authorization": f"token {token}"}
        response = requests.post(url, headers=headers)
        if response.status_code == 202:  # 202 Accepted indicates forking initiated
            return response.json()['html_url']
        else:
            return {"error": f"Failed to fork repository: {response.json().get('message', 'Unknown error')}"}


    @staticmethod
    def _update_github_repository(token: str, username: str, repo_name: str, description: str=None, private: bool=None):
        """
        Update an existing GitHub repository's description or privacy setting.

        :param token: Personal access token for the GitHub API
        :param username: GitHub username
        :param repo_name: Name of the repository
        :param description: New description for the repository
        :param private: Boolean indicating whether the repository should be private
        :return: A response indicating success or failure
        """
        url = f"https://api.github.com/repos/{username}/{repo_name}"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }

        # Constructing the data payload
        data = {}
        if description is not None:
            data['description'] = description
        if private is not None:
            data['private'] = private

        # Only send a request if there's something to update
        if data:
            response = requests.patch(url, headers=headers, json=data)
            if response.status_code in [200, 202]:  # 200 OK or 202 Accepted
                return {"response": f"Repository '{repo_name}' updated successfully!"}
            else:
                return {"error": f"Failed to update repository: {response.json()}"}
        else:
            return {"response": "No updates to perform."}


    @staticmethod
    def create_github_remote_repo(auth: AuthAgent, repository_name: str, description:str = None, private: bool = None, gh_remote_url: str = None):
        gh_user = auth.gh_user
        gh_pat = auth.gh_pat
        if not gh_user:
            return {"error": "Github user not set when calling API."}
        if not gh_pat:
            return {"error": "Github personal access token not set when calling API."}
        # Check if the repository already exists under the user's account
        if not RepositoryService._check_repo_exists(gh_user, repository_name, gh_pat):
            # If the repository does not exist, create it
            # check if we need to fork the repository
            if gh_remote_url:
                remote_gh_user = RepositoryService._get_username_from_repo_url(gh_remote_url)
                if 'error' in remote_gh_user:
                    return remote_gh_user
            # if the user's are different it means we are dealing with another remote so we fork
            if gh_remote_url and remote_gh_user != gh_user:
                if not RepositoryService._check_repo_exists(remote_gh_user, repository_name, gh_pat):
                    return {"error": f"Repository({repository_name}) does not exist remotely."}
                # If the repository exists and belongs to a different user, fork it
                gh_remote_url = RepositoryService._fork_repository(gh_pat, f"{remote_gh_user}/{repository_name}")
                if 'error' in gh_remote_url:
                    return gh_remote_url
                if not RepositoryService._check_repo_exists(gh_user, repository_name, gh_pat):
                    return {"error": f"After forking repository({repository_name}) could not locate remote under user {gh_user}."}
            else:
                create_response = RepositoryService._create_github_repository(gh_pat, repository_name, description or "", private or False)
                if 'error' in create_response:
                    return create_response
        else:
            update_response = RepositoryService._update_github_repository(gh_pat, gh_user, repository_name, description, private)
            if 'error' in update_response:
                return update_response
        return f"https://github.com/{gh_user}/{repository_name}.git"

    
    @staticmethod
    def _get_username_from_repo_url(repo_url: str):
        """
        Extract the username from a GitHub repository URL.

        :param repo_url: The full URL of the GitHub repository
        :return: Username of the repository owner
        """
        # Check for common Git URL formats: HTTPS and SSH
        if repo_url.startswith("https://"):
            # HTTPS URL format: https://github.com/username/repo_name
            parts = repo_url.split('/')
            if 'github.com' in parts and len(parts) > 3:
                return parts[3]  # Username is the fourth element
        elif repo_url.startswith("git@"):
            # SSH URL format: git@github.com:username/repo_name.git
            parts = repo_url.split(':')
            if len(parts) == 2:
                subparts = parts[1].split('/')
                if len(subparts) > 1:
                    return subparts[0]  # Username is before the repo name
        else:
            return {"error": "Invalid or unsupported Git URL format"}

        return {"error": "Username could not be extracted from the URL"}
    
    @staticmethod
    def create_github_pull_request(auth: AuthAgent, repository_name: str, title: str, body: str, head_branch: str):
        """
        Create a pull request on GitHub, automatically detecting if it's a fork.

        :param token: Personal access token for the GitHub API
        :param repo: Repository name with owner (e.g., "fork-owner/repo")
        :param title: Title of the pull request
        :param body: Content of the pull request
        :param head_branch: Name of the branch where your changes are implemented
        """
        repo = f"{auth.gh_user}/{repository_name}"
        url = f"https://api.github.com/repos/{repo}"
        headers = {"Authorization": f"token {auth.gh_pat}", "Accept": "application/vnd.github.v3+json"}

        # Get repository details to check if it's a fork and find the parent repo
        repo_details = requests.get(url, headers=headers).json()
        if 'parent' in repo_details:
            parent_repo = repo_details['parent']['full_name']  # format: "parent-owner/repo"
            head = f"{repo.split('/')[0]}:{head_branch}"  # format: "fork-owner:branch"
        else:
            parent_repo = repo
            head = head_branch

        # Create the pull request
        pr_url = f"https://api.github.com/repos/{parent_repo}/pulls"
        pr_data = {"title": title, "body": body, "head": head, "base": "main"}
        response = requests.post(pr_url, headers=headers, json=pr_data)
        if response.status_code == 201:
            return f"Pull request created successfully, URL: {response.json()['html_url']}"
        else:
            return {"error": f"Failed to create pull request: {response.content}"}

    @staticmethod
    def clone_repo(auth: AuthAgent, gh_remote_url: str, workspace: Path):
        # Clone the repository if it's not already cloned
        repo = GitRepository(local_path=workspace, auto_init=False)
        is_cloned = RepositoryService._is_repo_cloned(repo, gh_remote_url)
        print(f'clone_repo repo {repo} is_cloned {is_cloned}')
        if not is_cloned:
            clone_response = RepositoryService._clone_repository(repo, gh_remote_url, workspace)
            if 'error' in clone_response:
                return clone_response
            print('_clone_repository done')
            is_cloned = RepositoryService._is_repo_cloned(repo, gh_remote_url)
            if not is_cloned:
                return {"error": f"Repository({gh_remote_url}) was not cloned."}
            print('_is_repo_cloned done')
            # Set remote URL with PAT for authentication
            remote_auth_url = RepositoryService._construct_github_remote_url_with_pat(auth.gh_user, auth.gh_pat, gh_remote_url)
            if 'error' in remote_auth_url:
                return remote_auth_url
            print('_construct_github_remote_url_with_pat done')
            set_remote_response = RepositoryService.execute_git_command(repo, f"remote set-url origin {remote_auth_url}")
            if 'error' in set_remote_response:
                return set_remote_response
            print('success')
            return {"response": f"Repository was successfully cloned + authorized using a Personal Access Token to remote: {gh_remote_url}."}
        else:
            print('already cloned')
            return {"response": "The repository was already cloned."}

    @staticmethod
    def _clone_repository(git_repo: GitRepository, repo_url: str, workspace: Path):
        try:
            git_repo._repository.clone_from(repo_url, workspace)
            return {"response": f"Repository cloned successfully to {workspace}"}
        except Exception as e:
            return {"error": f"Error cloning repository: {e}"}
            
    @staticmethod
    def _is_repo_cloned(git_repo: GitRepository, remote_url: str):
        """
        Check if the GitPython Repo object is associated with the given remote URL.

        :param repo: GitPython Repo object
        :param remote_url: URL of the remote repository to check
        :return: True if the Repo is cloned from the remote URL, False otherwise
        """
        if not remote_url or not git_repo._repository:
            return False
        try:
            for remote in git_repo._repository.remotes:
                for url in remote.urls:
                    if url == remote_url:
                        return True
            return False
        except Exception as e:
            return False

    @staticmethod
    def _construct_github_remote_url_with_pat(gh_user: str, gh_pat: str, gh_remote_url: str):
        """
        Embed the GitHub username and PAT into the GitHub remote URL.

        :param gh_user: GitHub username
        :param gh_pat: GitHub Personal Access Token
        :param gh_remote_url: Original GitHub remote URL
        :return: GitHub remote URL with embedded username and PAT
        """
        # Split the original URL to insert username and PAT
        url_parts = gh_remote_url.split('://')
        if len(url_parts) != 2 or not url_parts[1].startswith("github.com"):
            return {"error": "Invalid GitHub remote URL"}

        # Construct the new URL with username and PAT
        new_url = f"https://{gh_user}:{gh_pat}@{url_parts[1]}"
        return new_url
    
    @staticmethod
    def execute_git_command(git_repo: GitRepository, git_command: str):
        try:
            if not git_repo._repository:
                return {"error": "No repository found to execute command against"}
            result = git_repo._repository.git.execute(git_command.split())
            return {"response": f"Git command executed successfully: {result}"}
        except Exception as e:
            return {"error": f"Error executing Git command: {e}"}
