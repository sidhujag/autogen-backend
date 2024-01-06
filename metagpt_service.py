import cachetools.func
import re
import os
import logging

from dotenv import load_dotenv
from functions_and_agents_metadata import AuthAgent
from pathlib import Path
from metagpt.config import CONFIG
from metagpt.roles import researcher, Architect, Engineer, ProductManager, ProjectManager, QaEngineer
from metagpt.team import Team
from metagpt.utils.common import (
    read_json_file
)

from metagpt.utils.git_repository import GitRepository
from typing import Iterator
from git.objects.commit import Commit

class MetaGPTService:
    
    SERDESER_PATH = Path()  # Initialize as an empty Path

    @staticmethod
    def _summarize_commits(commit_iterator: Iterator[Commit]) -> str:
        summary_lines = []

        for commit in commit_iterator:
            # Extract relevant information from each commit
            commit_sha = commit.hexsha[:7]  # Short SHA
            author = commit.author.name
            date = commit.authored_datetime.strftime("%Y-%m-%d")
            message = commit.summary  # First line of the commit message

            # Get file changes (added, removed, modified)
            file_changes = []
            for file, stats in commit.stats.files.items():
                change = f"{file} (insertions: {stats['insertions']}, deletions: {stats['deletions']})"
                file_changes.append(change)

            # Join file changes and format the information into a string
            file_changes_str = "; ".join(file_changes)
            summary_line = f"Commit: {commit_sha}, Author: {author}, Date: {date}, Message: {message}, Changes: {file_changes_str}"
            summary_lines.append(summary_line)

        # Join all summaries into a single string
        return "\n".join(summary_lines)
    
    @staticmethod
    def create_company():
        company = Team()
        company.hire(
            [
                ProductManager(),
                Architect(),
                ProjectManager(),
                Engineer(n_borg=5, use_code_review=True),
                QaEngineer() 
            ]
        )
        return company

    async def run_company(self, company: Team, n_round=5, idea="", send_to=""):
        """Run company until target round or no money"""
        if idea:
            company.run_project(idea=idea, send_to=send_to)

        while n_round > 0:
            # self._save()
            n_round -= 1
            logging.debug(f"max {n_round=} left.")
            company._check_balance()

            await company.env.run()
        if CONFIG.git_repo:
            CONFIG.git_repo.archive()
        return company.env.history

    async def run(self, auth: AuthAgent, workspace: Path, project_name: str, reqa_file: str, command_message: str):
        load_dotenv()  # Load environment variables
        serpkey = os.getenv("SERPAPI_API_KEY")
        inc = True
        max_auto_summarize_code = 0

        # Update the SERDESER_PATH in the metagpt.const module
        MetaGPTService.SERDESER_PATH = workspace / "storage" / "team"
        # Other logic
        CONFIG.update_via_cli(workspace, project_name, inc, reqa_file, max_auto_summarize_code)
        CONFIG.openai_api_key = auth.api_key
        CONFIG.serpapi_api_key = serpkey
        
        company = MetaGPTService.create_company()
        if not company:
            return {"error": f"MetaGPT coding assistant object not found: {project_name}"}
        company.run_project(command_message)
        try:
            await self.run_company(company)
        except KeyboardInterrupt:
            logging.error("KeyboardInterrupt occurs, start to serialize the project")
        except Exception as e:
            logging.error(f"Exception occurs, start to serialize the project: {e}")
        finally:
            company.serialize(MetaGPTService.SERDESER_PATH)

        history_file = MetaGPTService.SERDESER_PATH / "environment" / "history.json"
        str_output = "Published messages between MetaGPT agents(last 1024 characters)\n"
        if Path(history_file).exists():
            history = read_json_file(history_file)
            content = history.get("content", "")
            str_output += content[-1024:] if len(content) > 1024 else content
        else:
            str_output += "No history found"
        repo = GitRepository(local_path=workspace, auto_init=False)
        if repo.is_valid:
            file_list = repo.get_files(relative_path=".")
            str_output += "\n\nFile List\n"
            str_output += "\n".join(file_list)
            commit_log_iter = repo._repository.iter_commits(max_count=5)
            summarized_log = MetaGPTService._summarize_commits(commit_log_iter)
            str_output += "\n\nUp to last 5 commits('Archive' is default message for auto-serialization after each natural language invocation)\n"
            str_output += summarized_log
        return str_output
    
    @staticmethod 
    async def web_research(topic: str):
        filename = re.sub(r'[\\/:"*?<>|]+', " ", topic)
        filename = filename.replace("\n", "")
        await researcher.Researcher().run(topic)
        return (researcher.RESEARCH_PATH / f"{filename}.md").read_text()