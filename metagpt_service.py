import re
from metagpt.roles import researcher
class MetaGPTService:
    @staticmethod 
    async def web_research(topic: str):
        filename = re.sub(r'[\\/:"*?<>|]+', " ", topic)
        filename = filename.replace("\n", "")
        await researcher.Researcher().run(topic)
        return (researcher.RESEARCH_PATH / f"{filename}.md").read_text()