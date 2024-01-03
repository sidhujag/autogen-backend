import cachetools.func
import metagpt
import asyncio
import re
import os
from dotenv import load_dotenv
from functions_and_agents_metadata import AuthAgent
from pathlib import Path
from metagpt.config import CONFIG
from metagpt.roles import researcher, Architect, Engineer, ProductManager, ProjectManager, QaEngineer
from metagpt.team import Team
from metagpt.utils.common import (
    read_json_file
)
from metagpt import const as metagpt_const

class MetaGPTService:
    @staticmethod
    @cachetools.func.ttl_cache(maxsize=1024, ttl=36000)
    def get_or_create_company(workspace: Path):
        recover_path = workspace / "storage" / "team"
        if recover_path.exists():
            company = Team.deserialize(stg_path=recover_path)
            return company
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
           
    @staticmethod 
    async def run(auth: AuthAgent, workspace: Path, project_name: str, reqa_file: str, command_message: str):
        load_dotenv()  # Load environment variables
        serpkey = os.getenv("SERPAPI_API_KEY")
        inc = True
        max_auto_summarize_code = 0

        # Update the SERDESER_PATH in the metagpt.const module
        metagpt_const.SERDESER_PATH = workspace / "storage"

        company = MetaGPTService.get_or_create_company(workspace)
        if not company:
            return {"error": f"MetaGPT coding assistant object not found: {project_name}"}
        
        # Other logic
        CONFIG.update_via_cli(workspace, project_name, inc, reqa_file, max_auto_summarize_code)
        CONFIG.openai_api_key = auth.api_key
        CONFIG.serpapi_api_key = serpkey
        company.run_project(command_message)
        await company.run()

        # Read history from the updated SERDESER_PATH
        history = read_json_file(metagpt_const.SERDESER_PATH.joinpath("history.json"))
        str_output = history.get("content")[:1024]
        return str_output
    
    @staticmethod 
    async def web_research(topic: str):
        filename = re.sub(r'[\\/:"*?<>|]+', " ", topic)
        filename = filename.replace("\n", "")
        await researcher.Researcher().run(topic)
        return (researcher.RESEARCH_PATH / f"{filename}.md").read_text()